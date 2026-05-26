use chrono::{DateTime, Utc};

use crate::io::NoCursor;
use crate::partition::{Partition, PartitionGroup};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StartFrom<C = NoCursor> {
    Earliest,
    #[default]
    Latest,
    Timestamp(DateTime<Utc>),
    After(C),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StopAt<C = NoCursor> {
    #[default]
    Never,
    CurrentEnd,
    Cursor(C),
}

/// Capability trait for subscriptions that can be told to resume from a
/// cursor. `CheckpointReader` calls `with_start(StartFrom::After(cursor))`
/// on the inner subscription when it has a stored checkpoint.
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

/// Capability trait for subscriptions that can be restricted to a set of
/// partitions. `CoordinatedReader` calls `with_partitions(group)` on the
/// inner subscription before spawning each per-lease worker so a single
/// fetch per poll can cover all owned lanes.
///
/// Sibling of [`StartableSubscription`]: same `Self -> Self` builder shape,
/// same role (capability marker the reader composes against).
pub trait PartitionableSubscription<C>: StartableSubscription<C> + Clone + Send + 'static {
    /// Restrict the subscription to a validated group of partitions sharing
    /// the same `partition_count`. The reader emits a single fetch per poll
    /// using a multi-partition filter instead of one fetch per partition.
    ///
    /// This is the primary method.
    fn with_partitions(self, group: PartitionGroup) -> Self;

    /// Restrict the subscription to a single partition. The default impl
    /// wraps the partition in a singleton `PartitionGroup` and delegates to
    /// [`PartitionableSubscription::with_partitions`]. Backends override
    /// only when a single-partition path is materially cheaper than the
    /// multi-partition path (rare).
    fn with_partition(self, partition: Partition) -> Self
    where
        Self: Sized,
    {
        self.with_partitions(PartitionGroup::singleton(partition))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestCursor(i64);

    #[test]
    fn start_from_default_is_latest() {
        let start: StartFrom = StartFrom::default();
        assert_eq!(start, StartFrom::Latest);
    }

    #[test]
    fn start_from_timestamp_variant() {
        let timestamp = Utc::now();
        let start: StartFrom = StartFrom::Timestamp(timestamp);

        assert_eq!(start, StartFrom::Timestamp(timestamp));
    }

    #[test]
    fn start_from_after_variant_carries_cursor() {
        let start = StartFrom::After(TestCursor(9));

        assert_eq!(start, StartFrom::After(TestCursor(9)));
    }

    #[test]
    fn stop_at_default_is_never() {
        let stop: StopAt = StopAt::default();

        assert_eq!(stop, StopAt::Never);
    }

    #[test]
    fn stop_at_current_end_variant() {
        let stop: StopAt<TestCursor> = StopAt::CurrentEnd;

        assert_eq!(stop, StopAt::CurrentEnd);
    }

    #[test]
    fn stop_at_cursor_variant_carries_cursor() {
        let stop = StopAt::Cursor(TestCursor(10));

        assert_eq!(stop, StopAt::Cursor(TestCursor(10)));
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

    use crate::io::NoCursor;
    use std::num::NonZeroU32;

    #[derive(Clone, Default)]
    struct PartitionableStub {
        group: Option<PartitionGroup>,
    }

    impl StartableSubscription<NoCursor> for PartitionableStub {
        fn with_start(self, _start: StartFrom<NoCursor>) -> Self {
            self
        }
    }

    impl PartitionableSubscription<NoCursor> for PartitionableStub {
        fn with_partitions(mut self, group: PartitionGroup) -> Self {
            self.group = Some(group);
            self
        }
    }

    fn _accepts_partitionable<T, C>(_sub: T)
    where
        T: PartitionableSubscription<C>,
    {
    }

    #[test]
    fn partitionable_subscription_is_super_trait_of_startable() {
        _accepts_partitionable(PartitionableStub::default());
    }

    #[test]
    fn with_partition_default_impl_delegates_to_singleton_with_partitions() {
        let count = NonZeroU32::new(4).unwrap();
        let partition = Partition::new(2, count).unwrap();

        let via_single = PartitionableStub::default().with_partition(partition);
        let via_group =
            PartitionableStub::default().with_partitions(PartitionGroup::singleton(partition));

        assert_eq!(via_single.group, via_group.group);
        assert_eq!(
            via_single.group.expect("group set").partitions(),
            &[partition]
        );
    }
}
