use crate::FederationProviderRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};

/// Used to track if all sources, including tableScan, plan inputs and
/// expressions, represents an un-ambiguous, none or a sole' [`crate::FederationProvider`].
pub enum ScanResult {
    None,
    Distinct(FederationProviderRef),
    Ambiguous,
}

impl ScanResult {
    pub fn merge(&mut self, other: Self) {
        match (&self, &other) {
            (_, ScanResult::None) => {}
            (ScanResult::None, _) => *self = other,
            (ScanResult::Ambiguous, _) | (_, ScanResult::Ambiguous) => {
                *self = ScanResult::Ambiguous;
            }
            (ScanResult::Distinct(provider), ScanResult::Distinct(other_provider)) => {
                if provider != other_provider {
                    *self = ScanResult::Ambiguous;
                }
            }
        }
    }

    pub fn add(&mut self, provider: Option<FederationProviderRef>) {
        self.merge(ScanResult::from(provider))
    }

    pub fn is_ambiguous(&self) -> bool {
        matches!(self, ScanResult::Ambiguous)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, ScanResult::None)
    }
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub fn unwrap(self) -> Result<Option<FederationProviderRef>> {
        match self {
            ScanResult::None => Ok(None),
            ScanResult::Distinct(provider) => Ok(Some(provider)),
            ScanResult::Ambiguous => Err(DataFusionError::External(
                "called `ScanResult::unwrap()` on a `Ambiguous` value".into(),
            )),
        }
    }

    pub fn check_recursion(&self) -> TreeNodeRecursion {
        if self.is_ambiguous() {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        }
    }
}

impl From<Option<FederationProviderRef>> for ScanResult {
    fn from(provider: Option<FederationProviderRef>) -> Self {
        match provider {
            Some(provider) => ScanResult::Distinct(provider),
            None => ScanResult::None,
        }
    }
}

impl PartialEq<Option<FederationProviderRef>> for ScanResult {
    fn eq(&self, other: &Option<FederationProviderRef>) -> bool {
        match (self, other) {
            (ScanResult::None, None) => true,
            (ScanResult::Distinct(provider), Some(other_provider)) => provider == other_provider,
            _ => false,
        }
    }
}

impl PartialEq for ScanResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScanResult::None, ScanResult::None) => true,
            (ScanResult::Distinct(provider1), ScanResult::Distinct(provider2)) => {
                provider1 == provider2
            }
            (ScanResult::Ambiguous, ScanResult::Ambiguous) => true,
            _ => false,
        }
    }
}

impl std::fmt::Debug for ScanResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "ScanResult::None"),
            Self::Distinct(provider) => write!(f, "ScanResult::Distinct({})", provider.name()),
            Self::Ambiguous => write!(f, "ScanResult::Ambiguous"),
        }
    }
}

impl Clone for ScanResult {
    fn clone(&self) -> Self {
        match self {
            ScanResult::None => ScanResult::None,
            ScanResult::Distinct(provider) => ScanResult::Distinct(provider.clone()),
            ScanResult::Ambiguous => ScanResult::Ambiguous,
        }
    }
}
