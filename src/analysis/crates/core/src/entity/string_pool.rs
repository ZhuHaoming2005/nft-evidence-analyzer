//! Interned string pool backed by a single owned copy per unique string.

use super::ids::StringId;
use ahash::{AHashMap, RandomState};

/// Fixed-seed hasher so lookup/intern agree for the process lifetime.
fn hash_str(s: &str) -> u64 {
    // RandomState::with_seeds is public and deterministic for a given process build.
    let state = RandomState::with_seeds(
        0xA1A2_A3A4_B5B6_C7C8,
        0xD9DA_DBDC_DEDF_E0E1,
        0x1122_3344_5566_7788,
        0x99AA_BBCC_DDEE_FF00,
    );
    state.hash_one(s)
}

/// Global intern table for names, URIs, and other repeated strings.
///
/// Each unique string is stored once in `strings`. The hash map only holds
/// candidate ids (equality-checked against the arena), so intern avoids the
/// previous double `String` allocation on every first insert.
#[derive(Clone, Debug, Default)]
pub struct StringPool {
    strings: Vec<String>,
    /// `hash(str)` → first string id (always equality-checked).
    by_hash: AHashMap<u64, StringId>,
    /// Exact side table used only for genuine 64-bit hash collisions.
    collisions: AHashMap<u64, Vec<StringId>>,
}

impl StringPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.strings.reserve(additional);
        self.by_hash.reserve(additional);
    }

    pub fn get(&self, id: StringId) -> &str {
        &self.strings[id as usize]
    }

    pub fn lookup(&self, s: &str) -> Option<StringId> {
        let hash = hash_str(s);
        let first = *self.by_hash.get(&hash)?;
        if self.strings[first as usize] == s {
            return Some(first);
        }
        self.collisions
            .get(&hash)?
            .iter()
            .copied()
            .find(|&id| self.strings[id as usize] == s)
    }

    /// Intern `s`, returning the existing id on duplicate.
    pub fn intern(&mut self, s: &str) -> StringId {
        let hash = hash_str(s);
        if let Some(&first) = self.by_hash.get(&hash) {
            if self.strings[first as usize] == s {
                return first;
            }
            if let Some(candidates) = self.collisions.get(&hash) {
                for &id in candidates {
                    if self.strings[id as usize] == s {
                        return id;
                    }
                }
            }
            let id = StringId::try_from(self.strings.len()).expect("too many interned strings");
            self.strings.push(s.to_owned());
            self.collisions.entry(hash).or_default().push(id);
            return id;
        }
        let id = StringId::try_from(self.strings.len()).expect("too many interned strings");
        self.strings.push(s.to_owned());
        self.by_hash.insert(hash, id);
        id
    }

    /// Empty (including whitespace-only) → `None`; otherwise intern the raw slice.
    pub fn intern_nonempty(&mut self, s: &str) -> Option<StringId> {
        if s.trim().is_empty() {
            None
        } else {
            Some(self.intern(s))
        }
    }

    /// Empty / blank after trim → `None`; otherwise intern the trimmed value.
    pub fn intern_nonblank(&mut self, s: &str) -> Option<StringId> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(self.intern(trimmed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StringPool;

    #[test]
    fn string_pool_intern_deduplicates() {
        let mut pool = StringPool::default();
        let a = pool.intern("alpha");
        let b = pool.intern("alpha");
        let c = pool.intern("beta");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(pool.get(a), "alpha");
        assert_eq!(pool.get(c), "beta");
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn string_pool_empty_helpers_return_none() {
        let mut pool = StringPool::default();
        assert_eq!(pool.intern_nonempty(""), None);
        assert_eq!(pool.intern_nonempty("   \t"), None);
        assert_eq!(pool.intern_nonblank(""), None);
        assert_eq!(pool.intern_nonblank(" \t "), None);

        let gamma = pool.intern_nonempty("gamma").expect("non-empty");
        assert_eq!(pool.get(gamma), "gamma");
        // nonempty does not trim: leading spaces are kept as part of the value
        let spaced = pool.intern_nonempty("  keep ").expect("spaces kept");
        assert_eq!(pool.get(spaced), "  keep ");

        let delta = pool.intern_nonblank("  delta  ").expect("non-blank");
        assert_eq!(pool.get(delta), "delta");
        assert_eq!(pool.intern_nonblank("delta"), Some(delta));
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn string_pool_lookup_matches_intern() {
        let mut pool = StringPool::default();
        let id = pool.intern("shared-uri");
        assert_eq!(pool.lookup("shared-uri"), Some(id));
        assert_eq!(pool.lookup("missing"), None);
    }
}
