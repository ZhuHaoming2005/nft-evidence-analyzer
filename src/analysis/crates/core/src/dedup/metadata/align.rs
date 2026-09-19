//! Anchor alignment: largest shared token id, else largest each side.

/// One contract's metadata anchors in **descending** token-id order.
///
/// `token_key` is an interned EVM token-id key (ignored unless both sides are EVM).
/// `document_id` indexes the prepared BM25 document for that anchor's canonical JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AnchorRef {
    pub token_key: u32,
    pub document_id: u32,
}

/// Select the document pair to compare for metadata judgment.
///
/// - Both EVM: largest shared `token_key` (anchors already descending → scan forward).
/// - Otherwise (any Solana side, or no shared token): each side's largest anchor
///   (first entry when descending).
pub fn select_documents(
    left_is_evm: bool,
    left_anchors: &[AnchorRef],
    right_is_evm: bool,
    right_anchors: &[AnchorRef],
) -> Option<(u32, u32)> {
    if left_anchors.is_empty() || right_anchors.is_empty() {
        return None;
    }
    if left_is_evm && right_is_evm {
        for left in left_anchors {
            if let Some(right) = right_anchors
                .iter()
                .find(|right| right.token_key == left.token_key)
            {
                return Some((left.document_id, right.document_id));
            }
        }
    }
    Some((left_anchors[0].document_id, right_anchors[0].document_id))
}

#[cfg(test)]
mod tests {
    use super::{AnchorRef, select_documents};

    fn refs(pairs: &[(u32, u32)]) -> Vec<AnchorRef> {
        pairs
            .iter()
            .map(|&(token_key, document_id)| AnchorRef {
                token_key,
                document_id,
            })
            .collect()
    }

    #[test]
    fn evm_selects_largest_shared_token() {
        // Descending token keys 3,2,1 on left; 4,3,1 on right → shared 3 then 1 → pick 3.
        let left = refs(&[(3, 30), (2, 20), (1, 10)]);
        let right = refs(&[(4, 41), (3, 31), (1, 11)]);
        assert_eq!(select_documents(true, &left, true, &right), Some((30, 31)));
    }

    #[test]
    fn no_shared_token_uses_both_max_documents() {
        let left = refs(&[(2, 20), (1, 10)]);
        let right = refs(&[(4, 40), (3, 30)]);
        assert_eq!(select_documents(true, &left, true, &right), Some((20, 40)));
    }

    #[test]
    fn solana_pair_always_uses_max_each_side() {
        // Same token_key values would be shared on EVM; Solana must ignore and use max.
        let left = refs(&[(9, 90), (1, 10)]);
        let right = refs(&[(9, 91), (2, 20)]);
        assert_eq!(
            select_documents(false, &left, false, &right),
            Some((90, 91))
        );
        assert_eq!(select_documents(true, &left, false, &right), Some((90, 91)));
    }

    #[test]
    fn empty_anchors_yield_none() {
        let left = refs(&[(1, 10)]);
        assert_eq!(select_documents(true, &left, true, &[]), None);
        assert_eq!(select_documents(true, &[], true, &left), None);
    }
}
