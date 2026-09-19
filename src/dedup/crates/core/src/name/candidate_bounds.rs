/// Lossless optimistic bounds for the production Jaro-Winkler verifier.
pub struct CandidateBounds;

impl CandidateBounds {
    const ROUNDING_EPSILON: f64 = 1e-12;

    pub fn minimum_multiset_overlap(
        left_len: usize,
        right_len: usize,
        threshold_pct: f64,
    ) -> usize {
        if left_len == 0 && right_len == 0 {
            return 0;
        }
        if threshold_pct.is_nan() || threshold_pct > 100.0 {
            return left_len.min(right_len).saturating_add(1);
        }
        if threshold_pct <= 0.0 {
            return 0;
        }
        let max_overlap = left_len.min(right_len);
        let mut low = 0usize;
        let mut high = max_overlap.saturating_add(1);
        while low < high {
            let middle = low + (high - low) / 2;
            if Self::reaches_threshold(
                Self::optimistic_from_overlap(left_len, right_len, middle),
                threshold_pct,
            ) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    }

    pub fn lengths_can_reach(left_len: usize, right_len: usize, threshold_pct: f64) -> bool {
        Self::reaches_threshold(
            Self::upper_bound_from_lengths(left_len, right_len),
            threshold_pct,
        )
    }

    fn reaches_threshold(upper_bound: f64, threshold_pct: f64) -> bool {
        upper_bound >= threshold_pct || threshold_pct - upper_bound <= Self::ROUNDING_EPSILON
    }

    fn optimistic_from_overlap(left_len: usize, right_len: usize, overlap: usize) -> f64 {
        if left_len == 0 || right_len == 0 || overlap == 0 {
            return 0.0;
        }
        let overlap = overlap.min(left_len).min(right_len) as f64;
        let jaro = (overlap / left_len as f64 + overlap / right_len as f64 + 1.0) / 3.0;
        let prefix = overlap.min(left_len.min(right_len).min(4) as f64);
        let similarity = if jaro > 0.7 {
            jaro + 0.1 * prefix * (1.0 - jaro)
        } else {
            jaro
        };
        similarity.min(1.0) * 100.0
    }

    fn upper_bound_from_lengths(left_len: usize, right_len: usize) -> f64 {
        if left_len == 0 || right_len == 0 {
            return if left_len == right_len { 100.0 } else { 0.0 };
        }
        let shorter = left_len.min(right_len) as f64;
        let longer = left_len.max(right_len) as f64;
        let max_jaro = (1.0 + shorter / longer + 1.0) / 3.0;
        let max_prefix = left_len.min(right_len).min(4) as f64;
        (max_jaro + 0.1 * max_prefix * (1.0 - max_jaro)).min(1.0) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::CandidateBounds;
    use rapidfuzz::distance::jaro_winkler;

    fn strings(max_len: usize) -> Vec<String> {
        let mut values = vec![String::new()];
        for len in 1..=max_len {
            for bits in 0..(1usize << len) {
                values.push(
                    (0..len)
                        .map(|position| {
                            if bits & (1 << position) == 0 {
                                'a'
                            } else {
                                'b'
                            }
                        })
                        .collect(),
                );
            }
        }
        values
    }

    fn multiset_overlap(left: &str, right: &str) -> usize {
        let mut left = left.chars().collect::<Vec<_>>();
        let mut right = right.chars().collect::<Vec<_>>();
        left.sort_unstable();
        right.sort_unstable();
        let (mut i, mut j, mut overlap) = (0, 0, 0);
        while i < left.len() && j < right.len() {
            match left[i].cmp(&right[j]) {
                std::cmp::Ordering::Equal => {
                    overlap += 1;
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }
        overlap
    }

    #[test]
    fn production_bounds_cover_exhaustive_small_alphabet_hits() {
        let values = strings(7);
        for threshold_pct in [0.0, 50.0, 80.0, 95.0, 98.0, 100.0] {
            for left in &values {
                for right in &values {
                    let score = jaro_winkler::similarity(left.chars(), right.chars()) * 100.0;
                    if score < threshold_pct {
                        continue;
                    }
                    assert!(CandidateBounds::lengths_can_reach(
                        left.chars().count(),
                        right.chars().count(),
                        threshold_pct
                    ));
                    assert!(
                        multiset_overlap(left, right)
                            >= CandidateBounds::minimum_multiset_overlap(
                                left.chars().count(),
                                right.chars().count(),
                                threshold_pct
                            ),
                        "missed overlap bound for {left:?} vs {right:?} at {score} (threshold={threshold_pct})"
                    );
                }
            }
        }
    }
}
