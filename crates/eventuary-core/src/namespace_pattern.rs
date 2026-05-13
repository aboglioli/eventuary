use crate::event::Event;
use crate::io::Filter;
use crate::namespace::Namespace;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NamespacePattern {
    Prefix(Namespace),
}

impl NamespacePattern {
    pub fn prefix(namespace: Namespace) -> Self {
        Self::Prefix(namespace)
    }

    pub fn matches_namespace(&self, namespace: &Namespace) -> bool {
        match self {
            Self::Prefix(prefix) => namespace.starts_with(prefix),
        }
    }
}

impl Filter for NamespacePattern {
    fn matches(&self, event: &Event) -> bool {
        self.matches_namespace(event.namespace())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::event::Event;
    use crate::io::Filter;
    use crate::payload::Payload;

    #[test]
    fn prefix_matches_descendants() {
        let pattern = NamespacePattern::prefix(Namespace::new("/billing").unwrap());
        assert!(pattern.matches_namespace(&Namespace::new("/billing").unwrap()));
        assert!(pattern.matches_namespace(&Namespace::new("/billing/invoices").unwrap()));
        assert!(!pattern.matches_namespace(&Namespace::new("/orders").unwrap()));
    }

    #[test]
    fn namespace_pattern_is_a_filter() {
        let pattern = NamespacePattern::prefix(Namespace::new("/billing").unwrap());
        let event = Event::create(
            "org",
            "/billing/invoices",
            "invoice.created",
            Payload::from_string("p"),
        )
        .unwrap();
        assert!(Filter::matches(&pattern, &event));
    }

    #[test]
    fn namespace_pattern_filter_rejects_non_matching() {
        let pattern = NamespacePattern::prefix(Namespace::new("/billing").unwrap());
        let event =
            Event::create("org", "/orders", "order.placed", Payload::from_string("p")).unwrap();
        assert!(!Filter::matches(&pattern, &event));
    }
}
