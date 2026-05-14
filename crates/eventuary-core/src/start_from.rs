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

    /// Seed this subscription with a collection of candidate start
    /// positions. Default behavior: pick the smallest `StartFrom::After(c)`
    /// from the vec and delegate to `with_start`. Other variants
    /// (Earliest, Latest, Timestamp) are ignored by the default impl —
    /// readers that support fan-in, dual historic+live consumption, or
    /// topology-aware resume must override.
    fn with_starts(self, starts: Vec<StartFrom<C>>) -> Self
    where
        C: Ord,
    {
        let min = starts
            .into_iter()
            .filter_map(|s| match s {
                StartFrom::After(c) => Some(c),
                _ => None,
            })
            .min();
        match min {
            Some(c) => self.with_start(StartFrom::After(c)),
            None => self,
        }
    }
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

    #[derive(Debug, Clone, Default)]
    struct StartableSub {
        start: StartFrom<i64>,
    }

    impl StartableSubscription<i64> for StartableSub {
        fn with_start(mut self, start: StartFrom<i64>) -> Self {
            self.start = start;
            self
        }
    }

    #[test]
    fn with_starts_picks_min_after_cursor() {
        let sub = StartableSub::default();
        let starts = vec![
            StartFrom::After(100_i64),
            StartFrom::After(50_i64),
            StartFrom::After(200_i64),
        ];

        let resumed = sub.with_starts(starts);

        assert_eq!(resumed.start, StartFrom::After(50_i64));
    }

    #[test]
    fn with_starts_empty_returns_unchanged() {
        let sub = StartableSub::default();

        let resumed = sub.with_starts(vec![]);

        assert_eq!(resumed.start, StartFrom::Latest);
    }

    #[test]
    fn with_starts_ignores_non_after_variants_in_default_impl() {
        let sub = StartableSub::default();

        let resumed = sub.with_starts(vec![StartFrom::Earliest, StartFrom::Latest]);

        assert_eq!(resumed.start, StartFrom::Latest);
    }
}
