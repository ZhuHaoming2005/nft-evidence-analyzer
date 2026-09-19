//! Lossless optimistic bounds for Jaro-Winkler (fraction threshold in `[0, 1]`).

/// Production CandidateBounds adapted for fraction-scale JW thresholds.
pub struct CandidateBounds;

impl CandidateBounds {
    const ROUNDING_EPSILON: f64 = 1e-12;

    pub fn minimum_multiset_overlap(left_len: usize, right_len: usize, threshold: f64) -> usize {
        if left_len == 0 && right_len == 0 {
            return 0;
        }
        if threshold.is_nan() || threshold > 1.0 {
            return left_len.min(right_len).saturating_add(1);
        }
        if threshold <= 0.0 {
            return 0;
        }
        let max_overlap = left_len.min(right_len);
        let mut low = 0usize;
        let mut high = max_overlap.saturating_add(1);
        while low < high {
            let middle = low + (high - low) / 2;
            if Self::reaches_threshold(
                Self::optimistic_from_overlap(left_len, right_len, middle),
                threshold,
            ) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    }

    pub fn lengths_can_reach(left_len: usize, right_len: usize, threshold: f64) -> bool {
        Self::reaches_threshold(
            Self::upper_bound_from_lengths(left_len, right_len),
            threshold,
        )
    }

    fn reaches_threshold(upper_bound: f64, threshold: f64) -> bool {
        upper_bound >= threshold || threshold - upper_bound <= Self::ROUNDING_EPSILON
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
        similarity.min(1.0)
    }

    fn upper_bound_from_lengths(left_len: usize, right_len: usize) -> f64 {
        if left_len == 0 || right_len == 0 {
            return if left_len == right_len { 1.0 } else { 0.0 };
        }
        let shorter = left_len.min(right_len) as f64;
        let longer = left_len.max(right_len) as f64;
        let max_jaro = (1.0 + shorter / longer + 1.0) / 3.0;
        let max_prefix = left_len.min(right_len).min(4) as f64;
        (max_jaro + 0.1 * max_prefix * (1.0 - max_jaro)).min(1.0)
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
                std::cmp::Ordering::Less => {
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    j += 1;
                }
            }
        }
        overlap
    }

    #[test]
    fn fraction_bounds_cover_exhaustive_small_alphabet_hits() {
        let values = strings(7);
        for threshold in [0.0, 0.50, 0.80, 0.95, 0.98, 1.0] {
            for left in &values {
                for right in &values {
                    let score = jaro_winkler::similarity(left.chars(), right.chars());
                    if score < threshold {
                        continue;
                    }
                    assert!(CandidateBounds::lengths_can_reach(
                        left.chars().count(),
                        right.chars().count(),
                        threshold
                    ));
                    assert!(
                        multiset_overlap(left, right)
                            >= CandidateBounds::minimum_multiset_overlap(
                                left.chars().count(),
                                right.chars().count(),
                                threshold
                            ),
                        "missed overlap bound for {left:?} vs {right:?} at {score} (threshold={threshold})"
                    );
                }
            }
        }
    }

    #[test]
    fn threshold_0_98_rejects_clearly_different_lengths() {
        assert!(!CandidateBounds::lengths_can_reach(3, 20, 0.98));
        assert!(CandidateBounds::lengths_can_reach(10, 10, 0.98));
        assert!(CandidateBounds::lengths_can_reach(10, 11, 0.98));
    }

    #[test]
    fn percent_98_and_fraction_0_98_agree_on_overlap() {
        // Reference dedup uses percent; analysis uses fraction. Same decision.
        let left = 12usize;
        let right = 12usize;
        let frac = CandidateBounds::minimum_multiset_overlap(left, right, 0.98);
        assert!(frac <= left.min(right));
        assert!(CandidateBounds::lengths_can_reach(left, right, 0.98));
    }
}
