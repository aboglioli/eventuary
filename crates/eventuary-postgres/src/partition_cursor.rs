use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use eventuary_core::io::{Cursor, CursorId, CursorOrder};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PgPartitionCursor {
    pub partition_count: NonZeroU16,
    pub partition_id: u16,
    pub sequence: i64,
}

impl PgPartitionCursor {
    pub fn new(partition_count: NonZeroU16, partition_id: u16, sequence: i64) -> Self {
        Self {
            partition_count,
            partition_id,
            sequence,
        }
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    pub fn partition_id(&self) -> u16 {
        self.partition_id
    }
}

impl Cursor for PgPartitionCursor {
    fn id(&self) -> CursorId {
        CursorId::partition(self.partition_count.get(), self.partition_id)
    }

    fn order_key(&self) -> CursorOrder {
        CursorOrder::from_i64(self.sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(count: u16, id: u16, sequence: i64) -> PgPartitionCursor {
        PgPartitionCursor::new(NonZeroU16::new(count).unwrap(), id, sequence)
    }

    #[test]
    fn id_uses_partition_cursor_id() {
        let c = cursor(64, 7, 0);
        assert_eq!(c.id().as_str(), "partition:64:7");
    }

    #[test]
    fn order_key_uses_sequence() {
        let c = cursor(4, 0, 42);
        assert_eq!(c.order_key(), CursorOrder::from_i64(42));
    }

    #[test]
    fn serializes_via_json() {
        let c = cursor(8, 3, 99);
        let json = serde_json::to_string(&c).unwrap();
        let back: PgPartitionCursor = serde_json::from_str(&json).unwrap();
        assert_eq!(back.partition_count.get(), 8);
        assert_eq!(back.partition_id, 3);
        assert_eq!(back.sequence, 99);
    }
}
