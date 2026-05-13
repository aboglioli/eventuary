use chrono::{DateTime, Utc};

use crate::io::NoCursor;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StartFrom<C = NoCursor> {
    Earliest,
    #[default]
    Latest,
    Timestamp(DateTime<Utc>),
    After(C),
}

/// Marker for subscriptions that can be told to resume from a cursor.
/// `CheckpointReader` calls `with_start(StartFrom::After(cursor))` on the
/// inner subscription when it has a stored checkpoint.
pub trait StartableSubscription<C>: Clone + Send + 'static {
    fn with_start(self, start: StartFrom<C>) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestCursor(i64);

    #[test]
    fn default_is_latest() {
        let s: StartFrom = StartFrom::default();
        assert_eq!(s, StartFrom::Latest);
    }

    #[test]
    fn timestamp_variant() {
        let t = Utc::now();
        let s: StartFrom = StartFrom::Timestamp(t);
        if let StartFrom::Timestamp(t2) = s {
            assert_eq!(t, t2);
        } else {
            panic!("expected timestamp variant");
        }
    }

    #[test]
    fn after_variant_carries_cursor() {
        let start = StartFrom::After(TestCursor(9));
        assert_eq!(start, StartFrom::After(TestCursor(9)));
    }
}
