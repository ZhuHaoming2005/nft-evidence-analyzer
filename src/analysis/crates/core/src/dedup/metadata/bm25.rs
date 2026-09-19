const K1: f64 = 1.2;
const B: f64 = 0.75;
const IDF_ONCE: f64 = std::f64::consts::LN_2;
const IDF_SHARED: f64 = 0.182_321_556_793_954_6;
const IDF_RATIO_SQUARED: f64 = (IDF_SHARED / IDF_ONCE) * (IDF_SHARED / IDF_ONCE);
const UPPER_BOUND_EPSILON: f64 = 1e-12;
const INLINE_FREQUENCIES: usize = 4;
const MIN_LENGTH_NORM: f64 = K1 * (1.0 - B);
const MAX_LENGTH_NORM: f64 = K1 * (1.0 - B + 2.0 * B);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThresholdDecision {
    pub matched: bool,
    pub zero_overlap_pruned: bool,
    pub upper_bound_pruned: bool,
}

#[derive(Clone, Debug)]
pub struct PreparedDocument {
    term_start: u32,
    term_len: u32,
    frequency_histogram: FrequencyHistogram,
    term_mask: [u64; 2],
    length: u32,
}

#[cfg(test)]
pub(crate) struct PreparedDocumentParts {
    pub document: PreparedDocument,
    pub terms: Vec<(u32, u32)>,
}

#[derive(Clone, Debug)]
enum FrequencyHistogram {
    Inline {
        len: u8,
        values: [(u32, u32); INLINE_FREQUENCIES],
    },
    Heap(Box<[(u32, u32)]>),
}

impl FrequencyHistogram {
    fn from_terms(terms: &[(u32, u32)]) -> Self {
        let mut inline = [(0_u32, 0_u32); INLINE_FREQUENCIES];
        let mut inline_len = 0;
        let mut heap = None::<Vec<(u32, u32)>>;
        for &(_, frequency) in terms {
            if let Some(values) = &mut heap {
                if let Some((_, count)) = values
                    .iter_mut()
                    .find(|(candidate, _)| *candidate == frequency)
                {
                    *count += 1;
                } else {
                    values.push((frequency, 1));
                }
                continue;
            }
            if let Some((_, count)) = inline[..inline_len]
                .iter_mut()
                .find(|(candidate, _)| *candidate == frequency)
            {
                *count += 1;
            } else if inline_len < INLINE_FREQUENCIES {
                inline[inline_len] = (frequency, 1);
                inline_len += 1;
            } else {
                let mut values = inline.to_vec();
                values.push((frequency, 1));
                heap = Some(values);
            }
        }
        if let Some(mut values) = heap {
            values.sort_unstable_by_key(|(frequency, _)| *frequency);
            Self::Heap(values.into_boxed_slice())
        } else {
            inline[..inline_len].sort_unstable_by_key(|(frequency, _)| *frequency);
            Self::Inline {
                len: inline_len as u8,
                values: inline,
            }
        }
    }

    fn as_slice(&self) -> &[(u32, u32)] {
        match self {
            Self::Inline { len, values } => &values[..usize::from(*len)],
            Self::Heap(values) => values,
        }
    }
}

impl PreparedDocument {
    #[cfg(test)]
    pub(crate) fn try_new<'a, E>(
        canonical: &'a str,
        mut intern: impl FnMut(&'a str) -> Result<u32, E>,
    ) -> Result<PreparedDocumentParts, E> {
        let mut scratch = Vec::new();
        let mut terms = Vec::new();
        let document = Self::try_new_into(canonical, &mut intern, &mut scratch, &mut terms)?;
        Ok(PreparedDocumentParts { document, terms })
    }

    #[cfg(test)]
    pub(crate) fn try_new_into<'a, E>(
        canonical: &'a str,
        mut intern: impl FnMut(&'a str) -> Result<u32, E>,
        scratch: &mut Vec<u32>,
        terms: &mut Vec<(u32, u32)>,
    ) -> Result<Self, E> {
        scratch.clear();
        visit_tokens(canonical, |token| {
            scratch.push(intern(token)?);
            Ok(())
        })?;
        scratch.sort_unstable();
        let length = scratch.len() as u32;
        let term_start = terms.len();
        let mut term_mask = [0_u64; 2];
        let mut index = 0;
        while index < scratch.len() {
            let mut end = index + 1;
            while end < scratch.len() && scratch[end] == scratch[index] {
                end += 1;
            }
            terms.push((scratch[index], (end - index) as u32));
            let mixed = u64::from(scratch[index]).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let bit = (mixed >> 57) as usize;
            term_mask[bit / 64] |= 1_u64 << (bit % 64);
            index = end;
        }
        let compact_terms = &terms[term_start..];
        let frequency_histogram = FrequencyHistogram::from_terms(compact_terms);
        Ok(Self {
            term_start: 0,
            term_len: compact_terms.len() as u32,
            frequency_histogram,
            term_mask,
            length,
        })
    }

    pub(crate) fn from_compact_terms(terms: &[(u32, u32)]) -> Self {
        let mut term_mask = [0_u64; 2];
        let mut length = 0_u32;
        for &(term, frequency) in terms {
            length = length.saturating_add(frequency);
            let mixed = u64::from(term).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let bit = (mixed >> 57) as usize;
            term_mask[bit / 64] |= 1_u64 << (bit % 64);
        }
        Self {
            term_start: 0,
            term_len: terms.len() as u32,
            frequency_histogram: FrequencyHistogram::from_terms(terms),
            term_mask,
            length,
        }
    }

    pub(crate) fn set_term_start(&mut self, start: u32) {
        self.term_start = start;
    }

    pub(crate) fn terms<'a>(&self, terms: &'a [(u32, u32)]) -> &'a [(u32, u32)] {
        let start = self.term_start as usize;
        &terms[start..start + self.term_len as usize]
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn term_range(&self) -> std::ops::Range<usize> {
        let start = self.term_start as usize;
        start..start + self.term_len as usize
    }
}

pub(crate) fn visit_tokens<'a, E>(
    canonical: &'a str,
    mut visit: impl FnMut(&'a str) -> Result<(), E>,
) -> Result<(), E> {
    if canonical.is_ascii() {
        let bytes = canonical.as_bytes();
        let mut start = 0;
        while start < bytes.len() {
            while start < bytes.len() && !bytes[start].is_ascii_alphanumeric() {
                start += 1;
            }
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_alphanumeric() {
                end += 1;
            }
            if start < end {
                visit(&canonical[start..end])?;
            }
            start = end;
        }
    } else {
        for token in canonical
            .split(|character: char| !character.is_alphanumeric())
            .filter(|token| !token.is_empty())
        {
            visit(token)?;
        }
    }
    Ok(())
}

pub(crate) fn lossless_prefix_len(frequencies_in_rarity_order: &[u32], threshold: f64) -> usize {
    if frequencies_in_rarity_order.is_empty() || threshold.is_nan() || threshold > 1.0 {
        return 0;
    }
    if threshold <= 0.0 {
        return frequencies_in_rarity_order.len();
    }
    let rounding_margin = 64.0 * f64::EPSILON * frequencies_in_rarity_order.len() as f64;

    let mut suffix_shared_max = frequencies_in_rarity_order
        .iter()
        .map(|&frequency| {
            let weighted = weight(frequency, MIN_LENGTH_NORM, IDF_SHARED);
            weighted * weighted
        })
        .sum::<f64>();
    let mut prefix_unique_min = 0.0;
    for (index, &frequency) in frequencies_in_rarity_order.iter().enumerate() {
        let shared = weight(frequency, MIN_LENGTH_NORM, IDF_SHARED);
        suffix_shared_max = (suffix_shared_max - shared * shared).max(0.0);
        let unique = weight(frequency, MAX_LENGTH_NORM, IDF_ONCE);
        prefix_unique_min += unique * unique;
        let denominator = prefix_unique_min + suffix_shared_max;
        let upper_bound = if denominator <= 0.0 {
            0.0
        } else {
            (suffix_shared_max / denominator).sqrt()
        };
        if upper_bound + UPPER_BOUND_EPSILON + rounding_margin < threshold {
            return index + 1;
        }
    }
    frequencies_in_rarity_order.len()
}

pub fn cosine_similarity(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
) -> f64 {
    let weights = PairWeights::new(left, right);
    let mut shared = SharedWeights::default();
    for_each_shared_term(left_terms, right_terms, |left_tf, right_tf| {
        shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
    });
    weights.score(shared)
}

pub fn similarity_at_least(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
    threshold: f64,
) -> ThresholdDecision {
    similarity_at_least_impl(left, left_terms, right, right_terms, threshold, false).0
}

/// Like [`similarity_at_least`], but returns the exact cosine when the pair matches.
///
/// Avoids a second full shared-term scan on the hit path.
pub fn similarity_score_if_at_least(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
    threshold: f64,
) -> Option<f64> {
    let (decision, score) =
        similarity_at_least_impl(left, left_terms, right, right_terms, threshold, false);
    if !decision.matched {
        return None;
    }
    Some(score.unwrap_or_else(|| cosine_similarity(left, left_terms, right, right_terms)))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn similarity_at_least_after_overlap_filter(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
    threshold: f64,
) -> ThresholdDecision {
    similarity_at_least_impl(left, left_terms, right, right_terms, threshold, true).0
}

fn similarity_at_least_impl(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
    threshold: f64,
    overlap_filter_passed: bool,
) -> (ThresholdDecision, Option<f64>) {
    if threshold <= 0.0 {
        return (
            ThresholdDecision {
                matched: true,
                zero_overlap_pruned: false,
                upper_bound_pruned: false,
            },
            None,
        );
    }
    if !overlap_filter_passed && !may_share_term(left, left_terms, right, right_terms) {
        return (
            ThresholdDecision {
                matched: false,
                zero_overlap_pruned: true,
                upper_bound_pruned: false,
            },
            None,
        );
    }
    let weights = PairWeights::new(left, right);
    if weights.initial_upper_bound_below_threshold(threshold) {
        return (
            ThresholdDecision {
                matched: false,
                zero_overlap_pruned: false,
                upper_bound_pruned: true,
            },
            None,
        );
    }
    if left_terms.len().saturating_mul(4) < right_terms.len()
        || right_terms.len().saturating_mul(4) < left_terms.len()
    {
        let mut shared = SharedWeights::default();
        for_each_shared_term(left_terms, right_terms, |left_tf, right_tf| {
            shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
        });
        return decision_at_least(&weights, shared, threshold, false);
    }

    let mut left_pos = 0;
    let mut right_pos = 0;
    let mut processed_left_norm = 0.0;
    let mut processed_right_norm = 0.0;
    let mut shared = SharedWeights::default();
    let mut comparisons = 0_u32;
    let mut iterative_bound = None::<Option<IterativeBound>>;
    while left_pos < left_terms.len() && right_pos < right_terms.len() {
        let (left_term, left_tf) = &left_terms[left_pos];
        let (right_term, right_tf) = &right_terms[right_pos];
        match left_term.cmp(right_term) {
            std::cmp::Ordering::Equal => {
                let left_once = weights.left_once(*left_tf);
                let right_once = weights.right_once(*right_tf);
                let left_squared = left_once * left_once;
                let right_squared = right_once * right_once;
                processed_left_norm += left_squared;
                processed_right_norm += right_squared;
                shared.add_with_norms(left_once, right_once, left_squared, right_squared);
                left_pos += 1;
                right_pos += 1;
            }
            std::cmp::Ordering::Less => {
                let left_once = weights.left_once(*left_tf);
                processed_left_norm += left_once * left_once;
                left_pos += 1;
            }
            std::cmp::Ordering::Greater => {
                let right_once = weights.right_once(*right_tf);
                processed_right_norm += right_once * right_once;
                right_pos += 1;
            }
        }
        comparisons += 1;
        if comparisons.is_multiple_of(8) {
            let bound = *iterative_bound.get_or_insert_with(|| weights.iterative_bound(threshold));
            if bound.is_some_and(|bound| {
                weights.upper_bound_below_threshold(
                    bound,
                    shared,
                    processed_left_norm,
                    processed_right_norm,
                )
            }) {
                return (
                    ThresholdDecision {
                        matched: false,
                        zero_overlap_pruned: false,
                        upper_bound_pruned: true,
                    },
                    None,
                );
            }
        }
    }
    decision_at_least(&weights, shared, threshold, false)
}

pub(crate) fn may_share_term(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
) -> bool {
    let term_ranges_are_disjoint = match (
        left_terms.first(),
        left_terms.last(),
        right_terms.first(),
        right_terms.last(),
    ) {
        (
            Some((left_first, _)),
            Some((left_last, _)),
            Some((right_first, _)),
            Some((right_last, _)),
        ) => left_last < right_first || right_last < left_first,
        _ => true,
    };
    if term_ranges_are_disjoint {
        return false;
    }
    !left
        .term_mask
        .iter()
        .zip(right.term_mask)
        .all(|(left, right)| left & right == 0)
}

#[derive(Clone, Copy, Debug, Default)]
struct SharedWeights {
    left_norm: f64,
    right_norm: f64,
    dot: f64,
    count: u32,
}

impl SharedWeights {
    fn add(&mut self, left_once: f64, right_once: f64) {
        self.add_with_norms(
            left_once,
            right_once,
            left_once * left_once,
            right_once * right_once,
        );
    }

    fn add_with_norms(
        &mut self,
        left_once: f64,
        right_once: f64,
        left_squared: f64,
        right_squared: f64,
    ) {
        self.left_norm += left_squared;
        self.right_norm += right_squared;
        self.dot += left_once * right_once;
        self.count += 1;
    }
}

struct PairWeights {
    left_length_norm: f64,
    right_length_norm: f64,
    left_unit_weight: f64,
    right_unit_weight: f64,
    left_base_norm: f64,
    right_base_norm: f64,
    left_shared_norm_upper: f64,
    right_shared_norm_upper: f64,
    left_term_len: u32,
    right_term_len: u32,
    bound_rounding_margin: f64,
}

#[derive(Clone, Copy)]
struct IterativeBound {
    left_full_norm_upper: f64,
    right_full_norm_upper: f64,
    threshold_squared: f64,
}

impl PairWeights {
    fn new(left: &PreparedDocument, right: &PreparedDocument) -> Self {
        let avgdl = f64::from(left.length + right.length) / 2.0;
        let left_length_norm = length_norm(left.length, avgdl);
        let right_length_norm = length_norm(right.length, avgdl);
        let left_unit_weight = weight(1, left_length_norm, IDF_ONCE);
        let right_unit_weight = weight(1, right_length_norm, IDF_ONCE);
        let shared_term_limit = left.term_len.min(right.term_len);
        let (left_base_norm, left_shared_norm_upper) = histogram_norms(
            left.frequency_histogram.as_slice(),
            left_length_norm,
            left_unit_weight,
            left.term_len - shared_term_limit,
            shared_term_limit,
        );
        let (right_base_norm, right_shared_norm_upper) = histogram_norms(
            right.frequency_histogram.as_slice(),
            right_length_norm,
            right_unit_weight,
            right.term_len - shared_term_limit,
            shared_term_limit,
        );
        Self {
            left_length_norm,
            right_length_norm,
            left_unit_weight,
            right_unit_weight,
            left_base_norm,
            right_base_norm,
            left_shared_norm_upper,
            right_shared_norm_upper,
            left_term_len: left.term_len,
            right_term_len: right.term_len,
            bound_rounding_margin: 64.0
                * f64::EPSILON
                * (f64::from(left.term_len) + f64::from(right.term_len) + 16.0),
        }
    }

    fn left_once(&self, frequency: u32) -> f64 {
        if frequency == 1 {
            self.left_unit_weight
        } else {
            weight(frequency, self.left_length_norm, IDF_ONCE)
        }
    }

    fn right_once(&self, frequency: u32) -> f64 {
        if frequency == 1 {
            self.right_unit_weight
        } else {
            weight(frequency, self.right_length_norm, IDF_ONCE)
        }
    }

    fn score(&self, shared: SharedWeights) -> f64 {
        if shared.count == 0 {
            return 0.0;
        }
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let left_norm_squared = (self.left_base_norm - reduction * shared.left_norm).max(0.0);
        let right_norm_squared = (self.right_base_norm - reduction * shared.right_norm).max(0.0);
        let denominator = (left_norm_squared * right_norm_squared).sqrt();
        if denominator <= 0.0 {
            0.0
        } else {
            IDF_RATIO_SQUARED * shared.dot / denominator
        }
    }

    fn score_at_least(&self, shared: SharedWeights, threshold: f64) -> bool {
        if shared.count == 0 {
            return 0.0 >= threshold;
        }
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let left_norm_squared = (self.left_base_norm - reduction * shared.left_norm).max(0.0);
        let right_norm_squared = (self.right_base_norm - reduction * shared.right_norm).max(0.0);
        let denominator_squared = left_norm_squared * right_norm_squared;
        let numerator = IDF_RATIO_SQUARED * shared.dot;
        if terminal_negative_below_threshold(
            numerator,
            denominator_squared,
            threshold,
            self.bound_rounding_margin,
        ) {
            return false;
        }
        let denominator = denominator_squared.sqrt();
        let score = if denominator <= 0.0 {
            0.0
        } else {
            numerator / denominator
        };
        score >= threshold
    }

    fn initial_upper_bound_below_threshold(&self, threshold: f64) -> bool {
        let rejection_threshold = threshold - UPPER_BOUND_EPSILON - self.bound_rounding_margin;
        if rejection_threshold.is_nan() || rejection_threshold <= 0.0 {
            return false;
        }
        let rejection_threshold = rejection_threshold.next_down();
        let left_shared = self.left_shared_norm_upper;
        let right_shared = self.right_shared_norm_upper;
        if left_shared <= 0.0 || right_shared <= 0.0 {
            return false;
        }

        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let left_reduction = (reduction * left_shared).next_up();
        let right_reduction = (reduction * right_shared).next_up();
        let minimum_left_norm = (self.left_base_norm - left_reduction).next_down();
        let minimum_right_norm = (self.right_base_norm - right_reduction).next_down();
        if minimum_left_norm <= 0.0 || minimum_right_norm <= 0.0 {
            return false;
        }

        // At most `min(left_terms, right_terms)` terms can be shared. The
        // stored top-k norms bound the squared norm of every such subset, and
        // Cauchy-Schwarz therefore gives dot^2 <= left_shared * right_shared.
        // All numerator operations round upward; denominator and threshold
        // operations round downward, so this can only retain extra pairs.
        let ratio_fourth = (IDF_RATIO_SQUARED * IDF_RATIO_SQUARED).next_up();
        let shared_norm_product = (left_shared * right_shared).next_up();
        let numerator_squared = (ratio_fourth * shared_norm_product).next_up();
        let denominator_squared = (minimum_left_norm * minimum_right_norm).next_down();
        let threshold_squared = (rejection_threshold * rejection_threshold).next_down();
        if denominator_squared <= 0.0 || threshold_squared <= 0.0 {
            return false;
        }
        numerator_squared < (threshold_squared * denominator_squared).next_down()
    }

    fn iterative_bound(&self, threshold: f64) -> Option<IterativeBound> {
        let rejection_threshold = threshold - UPPER_BOUND_EPSILON - self.bound_rounding_margin;
        if !rejection_threshold.is_finite() || rejection_threshold <= 0.0 {
            return None;
        }
        let threshold_squared = (rejection_threshold * rejection_threshold).next_down();
        if !threshold_squared.is_finite() || threshold_squared <= 0.0 {
            return None;
        }
        Some(IterativeBound {
            left_full_norm_upper: full_norm_upper(self.left_base_norm, self.left_term_len),
            right_full_norm_upper: full_norm_upper(self.right_base_norm, self.right_term_len),
            threshold_squared,
        })
    }

    fn upper_bound_below_threshold(
        &self,
        bound: IterativeBound,
        shared: SharedWeights,
        processed_left_norm: f64,
        processed_right_norm: f64,
    ) -> bool {
        // `left_base_norm`/`right_base_norm` are accumulated by frequency
        // histogram, while the processed norms below follow term-id order.
        // Their rounding errors can therefore differ by more than one ULP for
        // very large documents. Use the same term-count-scaled guard as the
        // initial bound so the iterative bound can only retain extra pairs.
        let remaining_left = (bound.left_full_norm_upper - processed_left_norm)
            .max(0.0)
            .next_up();
        let remaining_right = (bound.right_full_norm_upper - processed_right_norm)
            .max(0.0)
            .next_up();
        let remaining_dot = (remaining_left * remaining_right).sqrt().next_up();
        let maximum_dot = (shared.dot + remaining_dot).next_up();
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let minimum_left_norm = (self.left_base_norm
            - reduction * (shared.left_norm + remaining_left))
            .max(0.0)
            .next_down();
        let minimum_right_norm = (self.right_base_norm
            - reduction * (shared.right_norm + remaining_right))
            .max(0.0)
            .next_down();
        if minimum_left_norm <= 0.0 || minimum_right_norm <= 0.0 {
            return false;
        }

        // All values are non-negative, so
        //
        //   numerator / sqrt(left_norm * right_norm) < threshold
        //
        // is equivalent to comparing their squares. Widen the numerator
        // upward and the rejection boundary downward so rounding can only
        // retain a candidate that the exact comparison could reject, never
        // reject an additional candidate. The existing threshold epsilon
        // remains outside the squared comparison.
        let numerator = (IDF_RATIO_SQUARED * maximum_dot).next_up();
        let numerator_squared = (numerator * numerator).next_up();
        let denominator_squared = (minimum_left_norm * minimum_right_norm).next_down();
        if denominator_squared <= 0.0 {
            return false;
        }
        numerator_squared < (bound.threshold_squared * denominator_squared).next_down()
    }
}

fn terminal_negative_below_threshold(
    numerator: f64,
    denominator_squared: f64,
    threshold: f64,
    rounding_margin: f64,
) -> bool {
    const TERMINAL_ROUNDING_GUARD: f64 = 16.0 * f64::EPSILON;

    if !numerator.is_finite()
        || numerator < 0.0
        || !denominator_squared.is_normal()
        || !threshold.is_normal()
    {
        return false;
    }
    let guarded_threshold = ((threshold - UPPER_BOUND_EPSILON - rounding_margin)
        * (1.0 - TERMINAL_ROUNDING_GUARD))
        .next_down();
    if !guarded_threshold.is_normal() || guarded_threshold <= 0.0 {
        return false;
    }

    // The strict, directed comparison proves the quotient is below a guard
    // that covers both correctly-rounded sqrt and division. Any boundary,
    // subnormal, overflow, or non-finite case falls back to the original path.
    let numerator_upper = numerator.next_up();
    let numerator_squared_upper = (numerator_upper * numerator_upper).next_up();
    let threshold_squared_lower = (guarded_threshold * guarded_threshold).next_down();
    let denominator_squared_lower = denominator_squared.next_down();
    let rejection_boundary = (threshold_squared_lower * denominator_squared_lower).next_down();
    numerator_squared_upper.is_finite()
        && rejection_boundary.is_finite()
        && numerator_squared_upper < rejection_boundary
}

#[cfg(test)]
fn decision(
    score: f64,
    threshold: f64,
    zero_overlap: bool,
    upper_bound_pruned: bool,
) -> ThresholdDecision {
    if zero_overlap && threshold <= 0.0 {
        return ThresholdDecision {
            matched: true,
            zero_overlap_pruned: false,
            upper_bound_pruned: false,
        };
    }
    ThresholdDecision {
        matched: score >= threshold,
        zero_overlap_pruned: zero_overlap && threshold > 0.0,
        upper_bound_pruned,
    }
}

fn decision_at_least(
    weights: &PairWeights,
    shared: SharedWeights,
    threshold: f64,
    upper_bound_pruned: bool,
) -> (ThresholdDecision, Option<f64>) {
    let zero_overlap = shared.count == 0;
    if zero_overlap && threshold <= 0.0 {
        return (
            ThresholdDecision {
                matched: true,
                zero_overlap_pruned: false,
                upper_bound_pruned: false,
            },
            Some(0.0),
        );
    }
    let matched = weights.score_at_least(shared, threshold);
    let score = matched.then(|| weights.score(shared));
    (
        ThresholdDecision {
            matched,
            zero_overlap_pruned: zero_overlap && threshold > 0.0,
            upper_bound_pruned,
        },
        score,
    )
}

#[cfg(test)]
fn legacy_cosine_similarity(
    left: &PreparedDocument,
    left_terms: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(u32, u32)],
) -> f64 {
    let avgdl = f64::from(left.length + right.length) / 2.0;
    let mut left_pos = 0;
    let mut right_pos = 0;
    let mut dot = 0.0;
    let mut left_norm_squared = 0.0;
    let mut right_norm_squared = 0.0;
    while left_pos < left_terms.len() || right_pos < right_terms.len() {
        match (left_terms.get(left_pos), right_terms.get(right_pos)) {
            (Some((left_term, left_tf)), Some((right_term, right_tf))) => {
                match left_term.cmp(right_term) {
                    std::cmp::Ordering::Equal => {
                        let left_weight = legacy_weight(*left_tf, left.length, avgdl, 2);
                        let right_weight = legacy_weight(*right_tf, right.length, avgdl, 2);
                        dot += left_weight * right_weight;
                        left_norm_squared += left_weight * left_weight;
                        right_norm_squared += right_weight * right_weight;
                        left_pos += 1;
                        right_pos += 1;
                    }
                    std::cmp::Ordering::Less => {
                        let left_weight = legacy_weight(*left_tf, left.length, avgdl, 1);
                        left_norm_squared += left_weight * left_weight;
                        left_pos += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        let right_weight = legacy_weight(*right_tf, right.length, avgdl, 1);
                        right_norm_squared += right_weight * right_weight;
                        right_pos += 1;
                    }
                }
            }
            (Some((_, left_tf)), None) => {
                let left_weight = legacy_weight(*left_tf, left.length, avgdl, 1);
                left_norm_squared += left_weight * left_weight;
                left_pos += 1;
            }
            (None, Some((_, right_tf))) => {
                let right_weight = legacy_weight(*right_tf, right.length, avgdl, 1);
                right_norm_squared += right_weight * right_weight;
                right_pos += 1;
            }
            (None, None) => break,
        }
    }
    if dot == 0.0 {
        return 0.0;
    }
    let left_norm = left_norm_squared.sqrt();
    let right_norm = right_norm_squared.sqrt();
    if left_norm <= 0.0 || right_norm <= 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

#[cfg(test)]
fn legacy_weight(frequency: u32, document_length: u32, avgdl: f64, document_frequency: u32) -> f64 {
    let frequency = f64::from(frequency);
    let document_length = f64::from(document_length);
    let document_frequency = f64::from(document_frequency);
    let idf = ((2.0 - document_frequency + 0.5) / (document_frequency + 0.5) + 1.0).ln();
    let tf_norm = (frequency * (K1 + 1.0))
        / (frequency + K1 * (1.0 - B + B * document_length / avgdl.max(1.0)));
    idf * tf_norm
}

fn for_each_shared_term(
    left: &[(u32, u32)],
    right: &[(u32, u32)],
    mut visit: impl FnMut(u32, u32),
) {
    let (shorter, longer, swapped) = if left.len() <= right.len() {
        (left, right, false)
    } else {
        (right, left, true)
    };
    if shorter.len().saturating_mul(4) < longer.len() {
        let mut longer_pos = 0;
        for (term, short_tf) in shorter {
            longer_pos = galloping_lower_bound(longer, longer_pos, *term);
            let Some(&(candidate, long_tf)) = longer.get(longer_pos) else {
                break;
            };
            if candidate != *term {
                continue;
            }
            if swapped {
                visit(long_tf, *short_tf);
            } else {
                visit(*short_tf, long_tf);
            }
            longer_pos += 1;
        }
        return;
    }

    let mut left_pos = 0;
    let mut right_pos = 0;
    while left_pos < left.len() && right_pos < right.len() {
        let (left_term, left_tf) = &left[left_pos];
        let (right_term, right_tf) = &right[right_pos];
        match left_term.cmp(right_term) {
            std::cmp::Ordering::Equal => {
                visit(*left_tf, *right_tf);
                left_pos += 1;
                right_pos += 1;
            }
            std::cmp::Ordering::Less => left_pos += 1,
            std::cmp::Ordering::Greater => right_pos += 1,
        }
    }
}

fn galloping_lower_bound(terms: &[(u32, u32)], start: usize, target: u32) -> usize {
    if start >= terms.len() || terms[start].0 >= target {
        return start;
    }

    let remaining = terms.len() - start;
    let mut step = 1_usize;
    while step < remaining && terms[start + step].0 < target {
        step = step.saturating_mul(2);
    }

    // `start` and every probed position through `step / 2` are below the
    // target. The first matching position, if any, is therefore in this
    // bounded monotonic window.
    let low = start + step / 2 + 1;
    let high = start
        .saturating_add(step)
        .saturating_add(1)
        .min(terms.len());
    low + terms[low..high].partition_point(|(term, _)| *term < target)
}

fn histogram_norms(
    histogram: &[(u32, u32)],
    length_norm: f64,
    unit_weight: f64,
    mut skipped_low_frequency_terms: u32,
    shared_term_limit: u32,
) -> (f64, f64) {
    let mut base_norm = 0.0;
    let mut top_norm_upper = 0.0;
    for &(frequency, count) in histogram {
        let weighted = if frequency == 1 {
            unit_weight
        } else {
            weight(frequency, length_norm, IDF_ONCE)
        };
        let squared = weighted * weighted;
        // This is the same ascending-frequency operation order used by the
        // former iterator sum, preserving the score's base norm bit-for-bit.
        base_norm += squared * f64::from(count);

        let skipped = skipped_low_frequency_terms.min(count);
        skipped_low_frequency_terms -= skipped;
        let selected = count - skipped;
        if selected > 0 {
            let squared_upper = squared.next_up();
            let contribution = (squared_upper * f64::from(selected)).next_up();
            top_norm_upper = (top_norm_upper + contribution).next_up();
        }
    }

    // `SharedWeights` accumulates one term at a time. Inflate the exact
    // top-k sum by a conservative positive-summation error bound, which also
    // covers the rounded products used by the shared dot product. Since
    // shared_term_limit <= u32::MAX, 4*k*epsilon remains far below one.
    let summation_inflation =
        (1.0 + 4.0 * (f64::from(shared_term_limit) + 1.0) * f64::EPSILON).next_up();
    top_norm_upper = (top_norm_upper * summation_inflation).next_up();
    (base_norm, top_norm_upper)
}

fn full_norm_upper(base_norm: f64, term_len: u32) -> f64 {
    // The base norm is grouped by frequency, whereas iterative scoring sums
    // the same non-negative squared weights in term-id order. Bound the
    // combined multiplication/addition error of both orders. Since term_len
    // is u32, n*epsilon remains below 1e-6 and this linear gamma bound is
    // safely outside both positive-summation error envelopes.
    let rounding_factor = (1.0 + 8.0 * (f64::from(term_len) + 1.0) * f64::EPSILON).next_up();
    (base_norm * rounding_factor).next_up()
}

fn length_norm(document_length: u32, avgdl: f64) -> f64 {
    K1 * (1.0 - B + B * f64::from(document_length) / avgdl.max(1.0))
}

fn weight(frequency: u32, length_norm: f64, idf: f64) -> f64 {
    let frequency = f64::from(frequency);
    let tf_norm = (frequency * (K1 + 1.0)) / (frequency + length_norm);
    idf * tf_norm
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;

    fn prepare(texts: &[&str]) -> Vec<PreparedDocumentParts> {
        let mut term_ids: AHashMap<String, u32> = AHashMap::new();
        texts
            .iter()
            .map(|text| {
                PreparedDocument::try_new(text, |term| {
                    if let Some(&id) = term_ids.get(term) {
                        return Ok::<_, std::convert::Infallible>(id);
                    }
                    let id = term_ids.len() as u32;
                    term_ids.insert(term.to_owned(), id);
                    Ok(id)
                })
                .unwrap()
            })
            .collect()
    }

    fn similarity(left: &PreparedDocumentParts, right: &PreparedDocumentParts) -> f64 {
        cosine_similarity(&left.document, &left.terms, &right.document, &right.terms)
    }

    fn legacy_similarity(left: &PreparedDocumentParts, right: &PreparedDocumentParts) -> f64 {
        legacy_cosine_similarity(&left.document, &left.terms, &right.document, &right.terms)
    }

    fn threshold_decision(
        left: &PreparedDocumentParts,
        right: &PreparedDocumentParts,
        threshold: f64,
    ) -> ThresholdDecision {
        similarity_at_least(
            &left.document,
            &left.terms,
            &right.document,
            &right.terms,
            threshold,
        )
    }

    fn prepared_document(terms: &[(u32, u32)]) -> PreparedDocument {
        let mut term_mask = [0_u64; 2];
        for &(term, _) in terms {
            let mixed = u64::from(term).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let bit = (mixed >> 57) as usize;
            term_mask[bit / 64] |= 1_u64 << (bit % 64);
        }
        PreparedDocument {
            term_start: 0,
            term_len: terms.len() as u32,
            frequency_histogram: FrequencyHistogram::from_terms(terms),
            term_mask,
            length: terms.iter().fold(0_u32, |length, (_, frequency)| {
                length.saturating_add(*frequency)
            }),
        }
    }

    fn linear_shared_frequencies(left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)> {
        let mut shared = Vec::new();
        let mut left_pos = 0;
        let mut right_pos = 0;
        while left_pos < left.len() && right_pos < right.len() {
            match left[left_pos].0.cmp(&right[right_pos].0) {
                std::cmp::Ordering::Equal => {
                    shared.push((left[left_pos].1, right[right_pos].1));
                    left_pos += 1;
                    right_pos += 1;
                }
                std::cmp::Ordering::Less => left_pos += 1,
                std::cmp::Ordering::Greater => right_pos += 1,
            }
        }
        shared
    }

    fn optimized_shared_frequencies(left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)> {
        let mut shared = Vec::new();
        for_each_shared_term(left, right, |left_tf, right_tf| {
            shared.push((left_tf, right_tf));
        });
        shared
    }

    fn linear_similarity_for_terms(
        left: &PreparedDocument,
        left_terms: &[(u32, u32)],
        right: &PreparedDocument,
        right_terms: &[(u32, u32)],
    ) -> (f64, bool) {
        let weights = PairWeights::new(left, right);
        let mut shared = SharedWeights::default();
        for (left_tf, right_tf) in linear_shared_frequencies(left_terms, right_terms) {
            shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
        }
        (weights.score(shared), shared.count == 0)
    }

    fn sqrt_upper_bound(
        weights: &PairWeights,
        shared: SharedWeights,
        processed_left_norm: f64,
        processed_right_norm: f64,
    ) -> f64 {
        let remaining_left = (weights.left_base_norm - processed_left_norm).max(0.0);
        let remaining_right = (weights.right_base_norm - processed_right_norm).max(0.0);
        let maximum_dot = shared.dot + (remaining_left * remaining_right).sqrt();
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let minimum_left_norm =
            (weights.left_base_norm - reduction * (shared.left_norm + remaining_left)).max(0.0);
        let minimum_right_norm =
            (weights.right_base_norm - reduction * (shared.right_norm + remaining_right)).max(0.0);
        let minimum_denominator = (minimum_left_norm * minimum_right_norm).sqrt();
        if minimum_denominator <= 0.0 {
            1.0
        } else {
            (IDF_RATIO_SQUARED * maximum_dot / minimum_denominator).min(1.0)
        }
    }

    fn legacy_histogram_norm(histogram: &[(u32, u32)], length_norm: f64, unit_weight: f64) -> f64 {
        histogram
            .iter()
            .map(|(frequency, count)| {
                let weighted = if *frequency == 1 {
                    unit_weight
                } else {
                    weight(*frequency, length_norm, IDF_ONCE)
                };
                weighted * weighted * f64::from(*count)
            })
            .sum()
    }

    fn next_random(state: &mut u64) -> u64 {
        *state ^= *state << 7;
        *state ^= *state >> 9;
        *state ^= *state << 8;
        *state
    }

    #[test]
    fn identical_texts_score_high() {
        let documents = prepare(&["hello world collection", "hello world collection"]);
        let left = &documents[0];
        let right = &documents[1];
        assert!(similarity(left, right) > 0.99);
    }

    #[test]
    fn overlap_filtered_threshold_path_matches_the_normal_path() {
        let documents = prepare(&[
            "shared alpha beta gamma left",
            "shared alpha beta delta right",
        ]);
        let left = &documents[0];
        let right = &documents[1];
        assert!(may_share_term(
            &left.document,
            &left.terms,
            &right.document,
            &right.terms
        ));
        for threshold in [0.2, 0.6, 0.95, 1.01] {
            assert_eq!(
                similarity_at_least_after_overlap_filter(
                    &left.document,
                    &left.terms,
                    &right.document,
                    &right.terms,
                    threshold,
                ),
                threshold_decision(left, right, threshold)
            );
        }
    }

    #[test]
    fn repeated_terms_allocate_only_unique_entries() {
        let document = prepare(&["name name name collection collection"])
            .pop()
            .unwrap();
        assert_eq!(document.document.length, 5);
        assert_eq!(document.terms.len(), 2);
        assert_eq!(
            document
                .terms
                .iter()
                .map(|(_, frequency)| *frequency)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
        assert_eq!(
            document.document.frequency_histogram.as_slice(),
            [(2, 1), (3, 1)]
        );
    }

    #[test]
    fn ascii_fast_tokenizer_matches_the_original_character_split() {
        let text = r#"{"name":"Alpha-42_beta","url":"https://example.com/a1"}"#;
        let expected = text
            .split(|character: char| !character.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        PreparedDocument::try_new(text, |term| {
            actual.push(term);
            Ok::<_, std::convert::Infallible>(actual.len() as u32)
        })
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn unicode_tokenizer_preserves_non_ascii_alphanumeric_terms() {
        let text = "猫 名称 café_42";
        let mut actual = Vec::new();
        PreparedDocument::try_new(text, |term| {
            actual.push(term);
            Ok::<_, std::convert::Infallible>(actual.len() as u32)
        })
        .unwrap();
        assert_eq!(actual, ["猫", "名称", "café", "42"]);
    }

    #[test]
    fn prepared_documents_read_terms_from_nonzero_csr_offsets() {
        let mut documents = prepare(&["alpha beta beta", "alpha gamma"]);
        let mut terms = vec![(u32::MAX, u32::MAX)];
        for parts in &mut documents {
            parts.document.set_term_start(terms.len() as u32);
            terms.extend_from_slice(&parts.terms);
        }
        let left_terms = documents[0].document.terms(&terms);
        let right_terms = documents[1].document.terms(&terms);
        assert_eq!(left_terms, documents[0].terms);
        assert_eq!(right_terms, documents[1].terms);
        assert_eq!(
            cosine_similarity(
                &documents[0].document,
                left_terms,
                &documents[1].document,
                right_terms,
            ),
            similarity(&documents[0], &documents[1])
        );
    }

    #[test]
    fn frequency_histogram_falls_back_without_changing_sorted_counts() {
        let document = prepare(&["a b b c c c d d d d e e e e e"]).pop().unwrap();
        assert!(matches!(
            &document.document.frequency_histogram,
            FrequencyHistogram::Heap(_)
        ));
        assert_eq!(
            document.document.frequency_histogram.as_slice(),
            [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]
        );
    }

    #[test]
    fn disjoint_documents_score_zero() {
        let documents = prepare(&["alpha beta gamma", "delta epsilon zeta"]);
        let left = &documents[0];
        let right = &documents[1];
        assert_eq!(similarity(left, right), 0.0);
    }

    #[test]
    fn imbalanced_documents_use_the_same_symmetric_score() {
        let documents = prepare(&[
            "shared",
            "a b c d e f g h i j k l m n o p q r s t u v shared",
        ]);
        let left = &documents[0];
        let right = &documents[1];
        assert_eq!(similarity(left, right), similarity(right, left));
    }

    #[test]
    fn galloping_intersection_matches_linear_merge_at_boundaries() {
        let longer = (0_u32..65)
            .map(|index| (10 + index * 3, index % 3 + 1))
            .collect::<Vec<_>>();
        let shorter = vec![
            (0, 2),
            (10, 2),
            (11, 3),
            (13, 2),
            (100, 3),
            (201, 2),
            (202, 3),
            (203, 2),
            (u32::MAX, 3),
        ];
        assert!(shorter.len() * 4 < longer.len());

        assert_eq!(galloping_lower_bound(&longer, 0, 0), 0);
        assert_eq!(galloping_lower_bound(&longer, 0, 10), 0);
        assert_eq!(galloping_lower_bound(&longer, 0, 11), 1);
        assert_eq!(galloping_lower_bound(&longer, 1, 202), 64);
        assert_eq!(galloping_lower_bound(&longer, 64, 203), longer.len());
        assert_eq!(
            galloping_lower_bound(&longer, longer.len(), u32::MAX),
            longer.len()
        );

        assert_eq!(
            optimized_shared_frequencies(&shorter, &longer),
            linear_shared_frequencies(&shorter, &longer)
        );
        assert_eq!(
            optimized_shared_frequencies(&longer, &shorter),
            linear_shared_frequencies(&longer, &shorter)
        );
        assert_eq!(optimized_shared_frequencies(&[], &longer), Vec::new());
        assert_eq!(optimized_shared_frequencies(&longer, &[]), Vec::new());
    }

    #[test]
    fn galloping_intersection_randomized_differential_is_bit_exact() {
        let mut state = 0x6a09_e667_f3bc_c909_u64;
        for case in 0..256 {
            let longer_len = 257 + (next_random(&mut state) as usize % 1_792);
            let mut term = (next_random(&mut state) % 4) as u32;
            let longer = (0..longer_len)
                .map(|_| {
                    term += 1 + (next_random(&mut state) % 4) as u32;
                    (term, 1 + (next_random(&mut state) % 7) as u32)
                })
                .collect::<Vec<_>>();
            let shorter_len = 1 + (next_random(&mut state) as usize % 32);
            let mut shorter = (0..shorter_len)
                .map(|_| {
                    let index = next_random(&mut state) as usize % longer.len();
                    let candidate = match next_random(&mut state) % 4 {
                        0 => longer[index].0,
                        1 => longer[index].0.saturating_sub(1),
                        2 => longer[index].0.saturating_add(1),
                        _ => (next_random(&mut state) % u64::from(term + 2)) as u32,
                    };
                    (candidate, 1 + (next_random(&mut state) % 7) as u32)
                })
                .collect::<Vec<_>>();
            shorter.sort_unstable_by_key(|(candidate, _)| *candidate);
            shorter.dedup_by_key(|(candidate, _)| *candidate);
            assert!(shorter.len() * 4 < longer.len());

            for (left_terms, right_terms) in [
                (shorter.as_slice(), longer.as_slice()),
                (longer.as_slice(), shorter.as_slice()),
            ] {
                let expected_shared = linear_shared_frequencies(left_terms, right_terms);
                assert_eq!(
                    optimized_shared_frequencies(left_terms, right_terms),
                    expected_shared,
                    "shared frequencies differ in case {case}"
                );

                let left = prepared_document(left_terms);
                let right = prepared_document(right_terms);
                let (expected_score, zero_overlap) =
                    linear_similarity_for_terms(&left, left_terms, &right, right_terms);
                let actual_score = cosine_similarity(&left, left_terms, &right, right_terms);
                assert_eq!(
                    actual_score.to_bits(),
                    expected_score.to_bits(),
                    "floating accumulation changed in case {case}"
                );

                for threshold in [0.0, 0.2, 0.6, expected_score, 0.95, 1.01] {
                    let actual = similarity_at_least_after_overlap_filter(
                        &left,
                        left_terms,
                        &right,
                        right_terms,
                        threshold,
                    );
                    assert_eq!(
                        actual.matched,
                        decision(expected_score, threshold, zero_overlap, false).matched,
                        "threshold result differs in case {case} at {threshold}"
                    );
                    if actual.upper_bound_pruned {
                        assert!(
                            expected_score < threshold,
                            "safe initial bound rejected a match in case {case} at {threshold}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn fused_histogram_top_k_bound_preserves_norms_and_matches_exhaustive_scores() {
        let mut state = 0x3c6e_f372_fe94_f82b_u64;
        let mut initial_rejections = 0_u32;
        let mut reordered_norm_exceeded_base = false;
        for case in 0..512 {
            let left_draws = 1 + next_random(&mut state) as usize % 96;
            let right_draws = 1 + next_random(&mut state) as usize % 384;
            let mut left_terms = (0..left_draws)
                .map(|_| {
                    (
                        (next_random(&mut state) % 512) as u32,
                        1 + (next_random(&mut state) % 11) as u32,
                    )
                })
                .collect::<Vec<_>>();
            let mut right_terms = (0..right_draws)
                .map(|_| {
                    (
                        (next_random(&mut state) % 512) as u32,
                        1 + (next_random(&mut state) % 11) as u32,
                    )
                })
                .collect::<Vec<_>>();
            left_terms.sort_unstable_by_key(|(term, _)| *term);
            left_terms.dedup_by_key(|(term, _)| *term);
            right_terms.sort_unstable_by_key(|(term, _)| *term);
            right_terms.dedup_by_key(|(term, _)| *term);

            let left = prepared_document(&left_terms);
            let right = prepared_document(&right_terms);
            let weights = PairWeights::new(&left, &right);
            assert_eq!(
                weights.left_base_norm.to_bits(),
                legacy_histogram_norm(
                    left.frequency_histogram.as_slice(),
                    weights.left_length_norm,
                    weights.left_unit_weight,
                )
                .to_bits(),
                "left base norm changed in case {case}"
            );
            assert_eq!(
                weights.right_base_norm.to_bits(),
                legacy_histogram_norm(
                    right.frequency_histogram.as_slice(),
                    weights.right_length_norm,
                    weights.right_unit_weight,
                )
                .to_bits(),
                "right base norm changed in case {case}"
            );
            let left_term_order_norm = left_terms.iter().fold(0.0, |norm, (_, frequency)| {
                let weighted = weights.left_once(*frequency);
                norm + weighted * weighted
            });
            let right_term_order_norm = right_terms.iter().fold(0.0, |norm, (_, frequency)| {
                let weighted = weights.right_once(*frequency);
                norm + weighted * weighted
            });
            reordered_norm_exceeded_base |= left_term_order_norm > weights.left_base_norm
                || right_term_order_norm > weights.right_base_norm;
            assert!(
                left_term_order_norm
                    <= full_norm_upper(weights.left_base_norm, weights.left_term_len)
            );
            assert!(
                right_term_order_norm
                    <= full_norm_upper(weights.right_base_norm, weights.right_term_len)
            );

            let mut shared = SharedWeights::default();
            for (left_tf, right_tf) in linear_shared_frequencies(&left_terms, &right_terms) {
                shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
            }
            assert!(shared.left_norm <= weights.left_shared_norm_upper);
            assert!(shared.right_norm <= weights.right_shared_norm_upper);
            assert!(
                shared.dot * shared.dot
                    <= (weights.left_shared_norm_upper * weights.right_shared_norm_upper).next_up()
            );

            let expected_score = weights.score(shared);
            for threshold in [
                0.2,
                0.6,
                expected_score.next_down(),
                expected_score,
                expected_score.next_up(),
                0.95,
            ] {
                let initial_reject = weights.initial_upper_bound_below_threshold(threshold);
                if initial_reject {
                    initial_rejections += 1;
                    assert!(
                        expected_score < threshold,
                        "initial top-k bound rejected a match in case {case} at {threshold}"
                    );
                }
                assert_eq!(
                    similarity_at_least_after_overlap_filter(
                        &left,
                        &left_terms,
                        &right,
                        &right_terms,
                        threshold,
                    )
                    .matched,
                    expected_score >= threshold,
                    "threshold result differs in case {case} at {threshold}"
                );
            }
        }
        assert!(initial_rejections > 0, "initial bound was never exercised");
        assert!(
            reordered_norm_exceeded_base,
            "the differential did not exercise histogram/term-order rounding"
        );
    }

    #[test]
    fn squared_upper_bound_is_conservative_and_threshold_exact() {
        let mut state = 0xbb67_ae85_84ca_a73b_u64;
        for case in 0..256 {
            let left_draws = 32 + next_random(&mut state) as usize % 33;
            let right_draws = 32 + next_random(&mut state) as usize % 33;
            let mut left_terms = (0..left_draws)
                .map(|_| {
                    (
                        (next_random(&mut state) % 256) as u32,
                        1 + (next_random(&mut state) % 9) as u32,
                    )
                })
                .collect::<Vec<_>>();
            let mut right_terms = (0..right_draws)
                .map(|_| {
                    (
                        (next_random(&mut state) % 256) as u32,
                        1 + (next_random(&mut state) % 9) as u32,
                    )
                })
                .collect::<Vec<_>>();
            left_terms.sort_unstable_by_key(|(term, _)| *term);
            left_terms.dedup_by_key(|(term, _)| *term);
            right_terms.sort_unstable_by_key(|(term, _)| *term);
            right_terms.dedup_by_key(|(term, _)| *term);
            assert!(left_terms.len().saturating_mul(4) >= right_terms.len());
            assert!(right_terms.len().saturating_mul(4) >= left_terms.len());

            let left = prepared_document(&left_terms);
            let right = prepared_document(&right_terms);
            let (expected_score, _) =
                linear_similarity_for_terms(&left, &left_terms, &right, &right_terms);
            let thresholds = [
                0.0,
                0.2,
                0.6,
                expected_score.next_down(),
                expected_score,
                expected_score.next_up(),
                0.95,
                1.01,
            ];
            for threshold in thresholds {
                assert_eq!(
                    similarity_at_least_after_overlap_filter(
                        &left,
                        &left_terms,
                        &right,
                        &right_terms,
                        threshold,
                    )
                    .matched,
                    expected_score >= threshold,
                    "threshold result differs in case {case} at {threshold}"
                );
            }

            let weights = PairWeights::new(&left, &right);
            let mut left_pos = 0;
            let mut right_pos = 0;
            let mut processed_left_norm = 0.0;
            let mut processed_right_norm = 0.0;
            let mut shared = SharedWeights::default();
            let mut comparisons = 0_u32;
            while left_pos < left_terms.len() && right_pos < right_terms.len() {
                let (left_term, left_tf) = left_terms[left_pos];
                let (right_term, right_tf) = right_terms[right_pos];
                match left_term.cmp(&right_term) {
                    std::cmp::Ordering::Equal => {
                        let left_once = weights.left_once(left_tf);
                        let right_once = weights.right_once(right_tf);
                        let left_squared = left_once * left_once;
                        let right_squared = right_once * right_once;
                        processed_left_norm += left_squared;
                        processed_right_norm += right_squared;
                        shared.add_with_norms(left_once, right_once, left_squared, right_squared);
                        left_pos += 1;
                        right_pos += 1;
                    }
                    std::cmp::Ordering::Less => {
                        let left_once = weights.left_once(left_tf);
                        processed_left_norm += left_once * left_once;
                        left_pos += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        let right_once = weights.right_once(right_tf);
                        processed_right_norm += right_once * right_once;
                        right_pos += 1;
                    }
                }
                comparisons += 1;
                if !comparisons.is_multiple_of(8) {
                    continue;
                }
                let sqrt_bound =
                    sqrt_upper_bound(&weights, shared, processed_left_norm, processed_right_norm);
                for threshold in thresholds {
                    let squared_reject = weights.iterative_bound(threshold).is_some_and(|bound| {
                        weights.upper_bound_below_threshold(
                            bound,
                            shared,
                            processed_left_norm,
                            processed_right_norm,
                        )
                    });
                    assert!(
                        !squared_reject || sqrt_bound + UPPER_BOUND_EPSILON < threshold,
                        "squared bound rejected more aggressively in case {case} at {threshold}"
                    );
                }
            }
        }
    }

    fn terminal_gate_rejects(weights: &PairWeights, shared: SharedWeights, threshold: f64) -> bool {
        let reduction = 1.0 - IDF_RATIO_SQUARED;
        let left_norm = (weights.left_base_norm - reduction * shared.left_norm).max(0.0);
        let right_norm = (weights.right_base_norm - reduction * shared.right_norm).max(0.0);
        terminal_negative_below_threshold(
            IDF_RATIO_SQUARED * shared.dot,
            left_norm * right_norm,
            threshold,
            weights.bound_rounding_margin,
        )
    }

    #[test]
    fn terminal_negative_gate_preserves_boundary_and_long_document_decisions() {
        assert!(!terminal_negative_below_threshold(f64::NAN, 1.0, 0.6, 0.0));
        assert!(!terminal_negative_below_threshold(
            0.1,
            f64::INFINITY,
            0.6,
            0.0
        ));
        assert!(!terminal_negative_below_threshold(
            0.1,
            f64::MIN_POSITIVE.next_down(),
            0.6,
            0.0
        ));
        let mut state = 0x3c6e_f372_fe94_f82b_u64;
        let mut gate_rejections = 0;
        for case in 0..512 {
            let left_len = 8 + next_random(&mut state) as usize % 57;
            let right_len = 8 + next_random(&mut state) as usize % 57;
            let mut left_terms = (0..left_len)
                .map(|_| {
                    (
                        (next_random(&mut state) % 512) as u32,
                        1 + (next_random(&mut state) % 8) as u32,
                    )
                })
                .collect::<Vec<_>>();
            let mut right_terms = (0..right_len)
                .map(|_| {
                    (
                        (next_random(&mut state) % 512) as u32,
                        1 + (next_random(&mut state) % 8) as u32,
                    )
                })
                .collect::<Vec<_>>();
            left_terms.sort_unstable_by_key(|term| term.0);
            left_terms.dedup_by_key(|term| term.0);
            right_terms.sort_unstable_by_key(|term| term.0);
            right_terms.dedup_by_key(|term| term.0);
            let left = prepared_document(&left_terms);
            let right = prepared_document(&right_terms);
            let weights = PairWeights::new(&left, &right);
            let mut shared = SharedWeights::default();
            for (left_tf, right_tf) in linear_shared_frequencies(&left_terms, &right_terms) {
                shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
            }
            let score = weights.score(shared);
            for threshold in [0.2, 0.6, score.next_down(), score, score.next_up(), 0.95] {
                let rejected = terminal_gate_rejects(&weights, shared, threshold);
                gate_rejections += usize::from(rejected);
                assert!(!rejected || score < threshold);
                assert_eq!(
                    weights.score_at_least(shared, threshold),
                    score >= threshold,
                    "terminal decision differs in case {case} at {threshold}"
                );
            }
        }

        let long_document = PreparedDocument {
            term_start: 0,
            term_len: 1_000_000,
            frequency_histogram: FrequencyHistogram::Inline {
                len: 2,
                values: [(1, 500_000), (2, 500_000), (0, 0), (0, 0)],
            },
            term_mask: [u64::MAX; 2],
            length: 1_500_000,
        };
        let other_long_document = PreparedDocument {
            term_start: 0,
            term_len: 1_000_000,
            frequency_histogram: FrequencyHistogram::Inline {
                len: 2,
                values: [(1, 333_333), (3, 666_667), (0, 0), (0, 0)],
            },
            term_mask: [u64::MAX; 2],
            length: 2_333_334,
        };
        let weights = PairWeights::new(&long_document, &other_long_document);
        let mut shared = SharedWeights::default();
        for index in 0..10_000 {
            let (left_tf, right_tf) = if index % 2 == 0 { (1, 3) } else { (2, 1) };
            shared.add(weights.left_once(left_tf), weights.right_once(right_tf));
        }
        let score = weights.score(shared);
        for threshold in [score.next_down(), score, score.next_up(), 0.95] {
            let rejected = terminal_gate_rejects(&weights, shared, threshold);
            gate_rejections += usize::from(rejected);
            assert!(!rejected || score < threshold);
            assert_eq!(
                weights.score_at_least(shared, threshold),
                score >= threshold
            );
        }
        assert!(gate_rejections > 0, "terminal gate was never exercised");
    }

    #[test]
    fn optimized_score_matches_legacy_score() {
        let documents = [
            "alpha beta gamma",
            "alpha alpha beta delta epsilon",
            "gamma delta zeta eta theta iota",
            "shared",
            "a b c d e f g h i j k l m n o p q r s t u v shared",
            "completely unrelated document terms",
        ];
        let documents = prepare(&documents);
        for left in &documents {
            for right in &documents {
                let optimized = similarity(left, right);
                let legacy = legacy_similarity(left, right);
                assert!(
                    (optimized - legacy).abs() <= 1e-12,
                    "optimized={optimized}, legacy={legacy}"
                );
                for threshold in [0.0, 0.2, 0.6, 0.95, 1.01] {
                    assert_eq!(
                        threshold_decision(left, right, threshold).matched,
                        legacy >= threshold
                    );
                }
            }
        }
    }

    #[test]
    fn upper_bound_prunes_low_overlap_documents() {
        let documents = prepare(&[
            "shared a01 a02 a03 a04 a05 a06 a07 a08 a09 a10 a11 a12 a13 a14 a15 a16",
            "shared z01 z02 z03 z04 z05 z06 z07 z08 z09 z10 z11 z12 z13 z14 z15 z16",
        ]);
        let left = &documents[0];
        let right = &documents[1];
        let decision = threshold_decision(left, right, 0.6);
        assert!(!decision.matched);
        assert!(decision.upper_bound_pruned || decision.zero_overlap_pruned);
        assert!(legacy_similarity(left, right) < 0.6);
    }

    #[test]
    fn threshold_pruning_matches_exhaustive_generated_oracle() {
        let vocabulary = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta",
        ];
        let texts = (1_u32..128)
            .map(|mask| {
                let mut words = Vec::new();
                for (index, word) in vocabulary.iter().enumerate() {
                    if mask & (1 << index) != 0 {
                        words.push(*word);
                        if (mask as usize + index).is_multiple_of(3) {
                            words.push(*word);
                        }
                    }
                }
                words.join(" ")
            })
            .collect::<Vec<_>>();
        let documents = prepare(&texts.iter().map(String::as_str).collect::<Vec<_>>());
        for left in &documents {
            for right in &documents {
                let legacy = legacy_similarity(left, right);
                for threshold in [0.2, 0.4, 0.6, 0.8, 0.95] {
                    assert_eq!(
                        threshold_decision(left, right, threshold).matched,
                        legacy >= threshold,
                        "legacy={legacy}, threshold={threshold}"
                    );
                }
            }
        }
    }

    #[test]
    fn lossless_prefix_contains_a_witness_for_every_threshold_match() {
        let texts = [
            "alpha beta gamma delta epsilon",
            "alpha beta gamma delta zeta",
            "alpha alpha beta gamma",
            "alpha alpha beta theta",
            "collection name attributes image",
            "collection title properties animation",
            "entirely unrelated vocabulary",
        ];
        let documents = prepare(&texts);
        let term_count = documents
            .iter()
            .flat_map(|document| document.terms.iter().map(|(term, _)| *term as usize + 1))
            .max()
            .unwrap_or(0);
        let mut document_frequency = vec![0_u32; term_count];
        for document in &documents {
            for &(term, _) in &document.terms {
                document_frequency[term as usize] += 1;
            }
        }
        let mut ordered_terms = document_frequency
            .iter()
            .enumerate()
            .map(|(term, frequency)| (*frequency, term as u32))
            .collect::<Vec<_>>();
        ordered_terms.sort_unstable();
        let mut rank = vec![0_u32; term_count];
        for (position, &(_, term)) in ordered_terms.iter().enumerate() {
            rank[term as usize] = position as u32;
        }
        let prefixes = documents
            .iter()
            .map(|document| {
                let mut terms = document.terms.clone();
                terms.sort_unstable_by_key(|(term, _)| rank[*term as usize]);
                let frequencies = terms
                    .iter()
                    .map(|(_, frequency)| *frequency)
                    .collect::<Vec<_>>();
                let len = lossless_prefix_len(&frequencies, 0.6);
                terms[..len]
                    .iter()
                    .map(|(term, _)| *term)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for (left_id, left) in documents.iter().enumerate() {
            for (right_id, right) in documents.iter().enumerate() {
                if legacy_similarity(left, right) < 0.6 {
                    continue;
                }
                assert!(
                    prefixes[left_id].iter().any(|term| {
                        right
                            .terms
                            .binary_search_by_key(term, |(candidate, _)| *candidate)
                            .is_ok()
                    }),
                    "left prefix has no witness for matching pair {left_id}/{right_id}"
                );
            }
        }
    }
}
