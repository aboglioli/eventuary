use crate::event_key::Partition;
use crate::io::position::StartableSubscription;

pub trait PartitionableSubscription<C>: StartableSubscription<C> + Clone + Send + 'static {
    fn with_partition(self, partition: Partition) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::position::StartFrom;
    use crate::io::{Cursor, NoCursor};

    #[derive(Clone)]
    struct StubSub;

    impl crate::io::position::StartableSubscription<NoCursor> for StubSub {
        fn with_start(self, _start: StartFrom<NoCursor>) -> Self {
            self
        }
    }

    impl PartitionableSubscription<NoCursor> for StubSub {
        fn with_partition(self, _partition: Partition) -> Self {
            self
        }
    }

    fn _accepts_partitionable<T, C>(_sub: T)
    where
        T: PartitionableSubscription<C>,
        C: Cursor,
    {
    }

    #[test]
    fn partitionable_subscription_is_super_trait_of_startable() {
        _accepts_partitionable(StubSub);
    }
}
