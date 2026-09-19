use rayon::prelude::*;
use std::mem::MaybeUninit;

const RADIX_BITS: u32 = 11;
const RADIX_SIZE: usize = 1 << RADIX_BITS;
const RADIX_MASK: u32 = (RADIX_SIZE as u32) - 1;
const PARALLEL_RADIX_MIN_LEN: usize = 32 * 1024;

#[derive(Clone, Copy)]
struct SharedOutput<T>(*mut MaybeUninit<T>);

// SharedOutput is used only with precomputed disjoint write ranges.
unsafe impl<T: Send> Send for SharedOutput<T> {}
// Concurrent access is write-only and every element has exactly one owner.
unsafe impl<T: Send> Sync for SharedOutput<T> {}

impl<T> SharedOutput<T> {
    unsafe fn write(&self, position: usize, value: T) {
        unsafe {
            self.0.add(position).write(MaybeUninit::new(value));
        }
    }
}

fn counting_pass<T: Copy>(
    source: &[T],
    destination: &mut [T],
    digit: impl Fn(T) -> usize,
    counts: &mut [usize],
) {
    counts.fill(0);
    for &value in source {
        counts[digit(value)] += 1;
    }
    let mut offset = 0;
    for count in counts.iter_mut() {
        let next = offset + *count;
        *count = offset;
        offset = next;
    }
    for &value in source {
        let bucket = digit(value);
        destination[counts[bucket]] = value;
        counts[bucket] += 1;
    }
}

fn counting_pass_uninit<T: Copy>(
    source: &[T],
    destination: &mut [MaybeUninit<T>],
    digit: impl Fn(T) -> usize,
    counts: &mut [usize],
) {
    counts.fill(0);
    for &value in source {
        counts[digit(value)] += 1;
    }
    let mut offset = 0;
    for count in counts.iter_mut() {
        let next = offset + *count;
        *count = offset;
        offset = next;
    }
    for &value in source {
        let bucket = digit(value);
        destination[counts[bucket]].write(value);
        counts[bucket] += 1;
    }
}

fn parallel_counting_pass<T: Copy + Send + Sync>(
    source: &[T],
    output_pointer: SharedOutput<T>,
    digit: &(impl Fn(T) -> usize + Sync),
    chunk_size: usize,
    local_counts: &mut [[usize; RADIX_SIZE]],
    chunk_offsets: &mut [[usize; RADIX_SIZE]],
) {
    local_counts
        .par_iter_mut()
        .zip(source.par_chunks(chunk_size))
        .for_each(|(counts, chunk)| {
            counts.fill(0);
            for &value in chunk {
                counts[digit(value)] += 1;
            }
        });

    let mut bucket_offsets = [0usize; RADIX_SIZE];
    let mut total = 0;
    for bucket in 0..RADIX_SIZE {
        bucket_offsets[bucket] = total;
        total += local_counts
            .iter()
            .map(|counts| counts[bucket])
            .sum::<usize>();
    }
    let mut next = bucket_offsets;
    for (offsets, counts) in chunk_offsets.iter_mut().zip(local_counts.iter()) {
        *offsets = next;
        for bucket in 0..RADIX_SIZE {
            next[bucket] += counts[bucket];
        }
    }

    source
        .par_chunks(chunk_size)
        .zip(chunk_offsets.par_iter())
        .for_each(|(chunk, offsets)| {
            let mut cursors = *offsets;
            for &value in chunk {
                let bucket = digit(value);
                let position = cursors[bucket];
                cursors[bucket] += 1;
                // Prefix sums reserve a unique destination range for every
                // (chunk, bucket), so concurrent writes cannot overlap.
                unsafe {
                    output_pointer.write(position, value);
                }
            }
        });
}

fn assume_init_vec<T>(mut values: Vec<MaybeUninit<T>>) -> Vec<T> {
    let pointer = values.as_mut_ptr().cast::<T>();
    let len = values.len();
    let capacity = values.capacity();
    std::mem::forget(values);
    // Every element is initialized exactly once by a complete counting pass,
    // and MaybeUninit<T> has the same layout as T.
    unsafe { Vec::from_raw_parts(pointer, len, capacity) }
}

fn sort_by_digits_while<T: Copy + Send + Sync>(
    values: &mut Vec<T>,
    digits: impl Fn(usize, T) -> usize + Sync,
    passes: usize,
    mut completed_pass: impl FnMut() -> bool,
) -> bool {
    if values.len() < 2 {
        return true;
    }
    if passes == 0 {
        return true;
    }
    let original = std::mem::take(values);
    let len = original.len();
    let parallel = len >= PARALLEL_RADIX_MIN_LEN && rayon::current_num_threads() > 1;
    let worker_chunks = rayon::current_num_threads().saturating_mul(4).max(1);
    let chunk_size = len.div_ceil(worker_chunks).max(1);
    let chunk_count = len.div_ceil(chunk_size);
    let mut local_counts = parallel.then(|| vec![[0usize; RADIX_SIZE]; chunk_count]);
    let mut chunk_offsets = parallel.then(|| vec![[0usize; RADIX_SIZE]; chunk_count]);

    let mut first_output = Vec::<MaybeUninit<T>>::with_capacity(len);
    // The first counting pass initializes every slot before any read.
    unsafe {
        first_output.set_len(len);
    }
    let first_digit = |value| digits(0, value);
    if parallel {
        parallel_counting_pass(
            &original,
            SharedOutput(first_output.as_mut_ptr()),
            &first_digit,
            chunk_size,
            local_counts
                .as_deref_mut()
                .expect("parallel radix counts are allocated"),
            chunk_offsets
                .as_deref_mut()
                .expect("parallel radix offsets are allocated"),
        );
    } else {
        let mut counts = [0usize; RADIX_SIZE];
        counting_pass_uninit(&original, &mut first_output, first_digit, &mut counts);
    }
    let mut source = assume_init_vec(first_output);
    if !completed_pass() {
        *values = source;
        return false;
    }

    let mut destination = original;
    for pass in 1..passes {
        let pass_digit = |value| digits(pass, value);
        if parallel {
            parallel_counting_pass(
                &source,
                SharedOutput(destination.as_mut_ptr().cast::<MaybeUninit<T>>()),
                &pass_digit,
                chunk_size,
                local_counts
                    .as_deref_mut()
                    .expect("parallel radix counts are allocated"),
                chunk_offsets
                    .as_deref_mut()
                    .expect("parallel radix offsets are allocated"),
            );
        } else {
            let mut counts = [0usize; RADIX_SIZE];
            counting_pass(&source, &mut destination, pass_digit, &mut counts);
        }
        std::mem::swap(&mut source, &mut destination);
        if !completed_pass() {
            *values = source;
            return false;
        }
    }
    *values = source;
    true
}

fn sort_by_digits_with_pass<T: Copy + Send + Sync>(
    values: &mut Vec<T>,
    digits: impl Fn(usize, T) -> usize + Sync,
    passes: usize,
    mut completed_pass: impl FnMut(),
) {
    let _ = sort_by_digits_while(values, digits, passes, || {
        completed_pass();
        true
    });
}

pub(crate) fn sort_by_digits<T: Copy + Send + Sync>(
    values: &mut Vec<T>,
    digits: impl Fn(usize, T) -> usize + Sync,
    passes: usize,
) {
    sort_by_digits_with_pass(values, digits, passes, || {});
}

pub(crate) fn u32_digit(value: u32, half: usize) -> usize {
    ((value >> (half as u32 * RADIX_BITS)) & RADIX_MASK) as usize
}

pub(crate) fn u64_digit(value: u64, quarter: usize) -> usize {
    ((value >> (quarter as u32 * RADIX_BITS)) & u64::from(RADIX_MASK)) as usize
}

#[cfg(test)]
pub(crate) fn sort_u64(values: &mut Vec<u64>) {
    sort_by_digits(values, |pass, value| u64_digit(value, pass), 6);
}

pub(crate) fn sort_u64_while(values: &mut Vec<u64>, completed_pass: impl FnMut() -> bool) -> bool {
    sort_by_digits_while(
        values,
        |pass, value| u64_digit(value, pass),
        6,
        completed_pass,
    )
}

pub(crate) fn sort_by_u32_bool_key_while<T: Copy + Send + Sync>(
    values: &mut Vec<T>,
    key: impl Fn(T) -> (u32, bool) + Sync,
    completed_pass: impl FnMut() -> bool,
) -> bool {
    sort_by_digits_while(
        values,
        |pass, value| {
            let (number, flag) = key(value);
            if pass == 0 {
                usize::from(flag)
            } else {
                u32_digit(number, pass - 1)
            }
        },
        4,
        completed_pass,
    )
}

#[cfg(test)]
pub(crate) fn sort_u32(values: &mut Vec<u32>) {
    sort_by_digits(values, |pass, value| u32_digit(value, pass), 3);
}

#[cfg(test)]
pub(crate) fn sort_u32_pairs(values: &mut Vec<(u32, u32)>) {
    sort_by_digits(
        values,
        |pass, value| {
            let (field, shift) = match pass {
                0..=2 => (value.1, pass as u32 * RADIX_BITS),
                _ => (value.0, (pass as u32 - 3) * RADIX_BITS),
            };
            u32_digit(field, (shift / RADIX_BITS) as usize)
        },
        6,
    );
}

pub(crate) fn sort_u32_pairs_while(
    values: &mut Vec<(u32, u32)>,
    completed_pass: impl FnMut() -> bool,
) -> bool {
    sort_by_digits_while(
        values,
        |pass, value| {
            let field = if pass <= 2 { value.1 } else { value.0 };
            u32_digit(field, pass % 3)
        },
        6,
        completed_pass,
    )
}

pub(crate) fn sort_u32_triples(values: &mut Vec<(u32, u32, u32)>) {
    sort_by_digits(
        values,
        |pass, value| {
            let field = match pass / 3 {
                0 => value.2,
                1 => value.1,
                _ => value.0,
            };
            let shift = (pass % 3) as u32 * RADIX_BITS;
            u32_digit(field, (shift / RADIX_BITS) as usize)
        },
        9,
    );
}

pub(crate) fn sort_u32_triples_while(
    values: &mut Vec<(u32, u32, u32)>,
    completed_pass: impl FnMut() -> bool,
) -> bool {
    sort_by_digits_while(
        values,
        |pass, value| {
            let field = match pass / 3 {
                0 => value.2,
                1 => value.1,
                _ => value.0,
            };
            u32_digit(field, pass % 3)
        },
        9,
        completed_pass,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_width_radix_orders_supported_keys() {
        let mut u32_values = vec![u32::MAX, 1, 0, 1 << 31];
        sort_u32(&mut u32_values);
        assert_eq!(u32_values, vec![0, 1, 1 << 31, u32::MAX]);

        let mut singles = vec![u64::MAX, 1, 0, u32::MAX as u64 + 1];
        sort_u64(&mut singles);
        assert_eq!(singles, vec![0, 1, u32::MAX as u64 + 1, u64::MAX]);

        let mut pairs = vec![(2, 0), (1, 4), (1, 2), (0, u32::MAX)];
        sort_u32_pairs(&mut pairs);
        assert_eq!(pairs, vec![(0, u32::MAX), (1, 2), (1, 4), (2, 0)]);

        let mut triples = vec![(1, 2, 1), (0, 9, 9), (1, 1, 5), (1, 1, 4)];
        sort_u32_triples(&mut triples);
        assert_eq!(triples, vec![(0, 9, 9), (1, 1, 4), (1, 1, 5), (1, 2, 1)]);
    }

    #[test]
    fn parallel_radix_matches_comparison_sort() {
        let mut state = 0x9e3779b97f4a7c15_u64;
        let mut values = (0..100_000)
            .map(|index| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                (state as u32, (state >> 32) as u32, index as u32)
            })
            .collect::<Vec<_>>();
        let mut expected = values.clone();
        expected.sort_unstable();
        sort_u32_triples(&mut values);
        assert_eq!(values, expected);
    }

    #[test]
    fn cancellable_radix_stops_at_a_pass_boundary_without_losing_values() {
        let mut values = vec![u64::MAX, 1, 0, u32::MAX as u64 + 1];
        let mut expected = values.clone();
        expected.sort_unstable();
        let mut completed_passes = 0;

        assert!(!sort_u64_while(&mut values, || {
            completed_passes += 1;
            false
        }));
        assert_eq!(completed_passes, 1);
        values.sort_unstable();
        assert_eq!(values, expected);
    }

    #[test]
    fn generic_u32_bool_key_matches_lexicographic_sort() {
        let mut values = vec![
            (2_u32, true, 0_u8),
            (1, true, 1),
            (2, false, 2),
            (1, false, 3),
        ];
        assert!(sort_by_u32_bool_key_while(
            &mut values,
            |value| (value.0, value.1),
            || true
        ));
        assert_eq!(
            values
                .iter()
                .map(|value| (value.0, value.1))
                .collect::<Vec<_>>(),
            vec![(1, false), (1, true), (2, false), (2, true)]
        );
    }
}
