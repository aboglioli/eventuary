use crate::namespace::Namespace;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NamespacePattern {
    Prefix(Namespace),
}

impl NamespacePattern {
    pub fn prefix(namespace: Namespace) -> Self {
        Self::Prefix(namespace)
    }

    pub fn matches(&self, namespace: &Namespace) -> bool {
        match self {
            Self::Prefix(prefix) => namespace.starts_with(prefix),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches_descendants() {
        let pattern = NamespacePattern::prefix(Namespace::new("/billing").unwrap());
        assert!(pattern.matches(&Namespace::new("/billing").unwrap()));
        assert!(pattern.matches(&Namespace::new("/billing/invoices").unwrap()));
        assert!(!pattern.matches(&Namespace::new("/orders").unwrap()));
    }
}
