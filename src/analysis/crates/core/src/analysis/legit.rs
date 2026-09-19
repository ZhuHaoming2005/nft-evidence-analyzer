//! Official migration / authorized reissue legit-duplicate classification.

use serde::{Deserialize, Serialize};

use crate::enrich::LegitSignals;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LegitClassification {
    pub is_legit_duplicate: bool,
    pub verification_complete: bool,
    pub evidence_keys: Vec<String>,
    pub reasons: Vec<String>,
}

pub fn classify(signals: &LegitSignals) -> LegitClassification {
    let mut reasons = Vec::new();
    if signals.seed_open_license {
        reasons.push("seed_open_license".into());
    }
    if signals.verified_migration {
        reasons.push("verified_migration".into());
    }
    if signals.authorized_reissue {
        reasons.push("authorized_reissue".into());
    }
    if signals.official_controller_continuity {
        reasons.push("official_controller_continuity".into());
    }
    if signals.official_collection_relation {
        reasons.push("official_collection_relation".into());
    }
    if signals.seed_nft_interaction {
        reasons.push("seed_nft_interaction".into());
    }
    LegitClassification {
        is_legit_duplicate: signals.is_legit_duplicate(),
        verification_complete: signals.verification_complete,
        evidence_keys: signals.evidence_keys.clone(),
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suspected_when_no_official_signals() {
        let out = classify(&LegitSignals::default());
        assert!(!out.is_legit_duplicate);
        assert!(!out.verification_complete);
        assert!(out.reasons.is_empty());
    }

    #[test]
    fn marks_legit_when_verified_migration_signal_set() {
        let out = classify(&LegitSignals {
            verified_migration: true,
            evidence_keys: vec!["migration:official".into()],
            verification_complete: true,
            ..LegitSignals::default()
        });
        assert!(out.is_legit_duplicate);
        assert_eq!(out.reasons, vec!["verified_migration".to_owned()]);
        assert_eq!(out.evidence_keys, vec!["migration:official".to_owned()]);
    }

    #[test]
    fn marks_legit_when_seed_has_open_license() {
        let out = classify(&LegitSignals {
            seed_open_license: true,
            evidence_keys: vec!["seed_open_license".into()],
            verification_complete: true,
            ..LegitSignals::default()
        });
        assert!(out.is_legit_duplicate);
        assert_eq!(out.reasons, vec!["seed_open_license".to_owned()]);
    }

    #[test]
    fn marks_legit_for_seed_nft_interaction() {
        let out = classify(&LegitSignals {
            seed_nft_interaction: true,
            evidence_keys: vec!["holds_seed_nft".into()],
            verification_complete: true,
            ..LegitSignals::default()
        });
        assert!(out.is_legit_duplicate);
        assert_eq!(out.reasons, vec!["seed_nft_interaction".to_owned()]);
    }
}
