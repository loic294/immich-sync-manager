//! Version vectors: causal history of a replicated file across instances.
//!
//! Each instance increments its own counter when it changes a file. Comparing two vectors tells
//! whether one change already saw the other (apply or ignore) or both happened independently
//! (a conflict). Wall clocks are never used to pick a winner.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VersionVector(pub BTreeMap<String, u64>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Causality {
    Equal,
    /// `self` has seen everything `other` has, and more.
    Dominates,
    /// `other` has seen everything `self` has, and more.
    DominatedBy,
    Concurrent,
}

impl VersionVector {
    pub fn get(&self, instance: &str) -> u64 {
        self.0.get(instance).copied().unwrap_or(0)
    }

    pub fn increment(&mut self, instance: &str) {
        *self.0.entry(instance.to_owned()).or_insert(0) += 1;
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (k, v) in &other.0 {
            let entry = self.0.entry(k.clone()).or_insert(0);
            *entry = (*entry).max(*v);
        }
    }

    pub fn compare(&self, other: &VersionVector) -> Causality {
        let mut greater = false;
        let mut less = false;
        for key in self.0.keys().chain(other.0.keys()) {
            match self.get(key).cmp(&other.get(key)) {
                Ordering::Greater => greater = true,
                Ordering::Less => less = true,
                Ordering::Equal => {}
            }
        }
        match (greater, less) {
            (false, false) => Causality::Equal,
            (true, false) => Causality::Dominates,
            (false, true) => Causality::DominatedBy,
            (true, true) => Causality::Concurrent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vv(pairs: &[(&str, u64)]) -> VersionVector {
        VersionVector(pairs.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect())
    }

    #[test]
    fn compares_causality() {
        assert_eq!(vv(&[("a", 1)]).compare(&vv(&[("a", 1)])), Causality::Equal);
        assert_eq!(
            vv(&[("a", 2)]).compare(&vv(&[("a", 1)])),
            Causality::Dominates
        );
        assert_eq!(
            vv(&[("a", 1)]).compare(&vv(&[("a", 1), ("b", 1)])),
            Causality::DominatedBy
        );
        assert_eq!(
            vv(&[("a", 2)]).compare(&vv(&[("a", 1), ("b", 1)])),
            Causality::Concurrent
        );
    }

    #[test]
    fn merge_takes_the_maximum() {
        let mut a = vv(&[("a", 2), ("b", 1)]);
        a.merge(&vv(&[("b", 3), ("c", 1)]));
        assert_eq!(a, vv(&[("a", 2), ("b", 3), ("c", 1)]));
        a.increment("a");
        assert_eq!(a.get("a"), 3);
    }
}
