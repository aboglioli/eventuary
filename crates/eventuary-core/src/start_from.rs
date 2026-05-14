use chrono::{DateTime, Utc};

use crate::io::{CursorId, NoCursor};

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

    fn with_resume_points(self, rows: Vec<(CursorId, C)>) -> Self
    where
        C: Ord,
    {
        let min = rows.into_iter().map(|(_, c)| c).min();
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
    fn with_resume_points_picks_min_cursor() {
        let sub = StartableSub::default();
        let rows = vec![
            (CursorId::Global, 100_i64),
            (CursorId::Named(std::sync::Arc::from("a")), 50_i64),
            (CursorId::Named(std::sync::Arc::from("b")), 200_i64),
        ];

        let resumed = sub.with_resume_points(rows);

        assert_eq!(resumed.start, StartFrom::After(50_i64));
    }

    #[test]
    fn with_resume_points_empty_returns_unchanged() {
        let sub = StartableSub::default();

        let resumed = sub.with_resume_points(vec![]);

        assert_eq!(resumed.start, StartFrom::Latest);
    }
}
