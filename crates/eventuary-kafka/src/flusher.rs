use std::collections::HashMap;
use std::sync::Arc;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};

use eventuary::io::acker::BatchFlusher;
use eventuary::{Error, Result};

#[derive(Clone, Debug)]
pub struct KafkaOffsetToken {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// ack commits the highest offset per partition via `consumer.commit`. nack is
/// a no-op: offsets are simply left uncommitted. Unacked records are
/// redelivered after rebalance, restart, or session expiry — there is no
/// immediate redelivery.
pub struct KafkaFlusher {
    consumer: Arc<StreamConsumer>,
}

impl KafkaFlusher {
    pub fn new(consumer: Arc<StreamConsumer>) -> Self {
        Self { consumer }
    }

    fn highest_per_partition(tokens: Vec<KafkaOffsetToken>) -> HashMap<(String, i32), i64> {
        let mut map: HashMap<(String, i32), i64> = HashMap::new();
        for t in tokens {
            let key = (t.topic, t.partition);
            map.entry(key)
                .and_modify(|cur| *cur = (*cur).max(t.offset))
                .or_insert(t.offset);
        }
        map
    }
}

impl BatchFlusher for KafkaFlusher {
    type Token = KafkaOffsetToken;

    async fn flush(&self, acks: Vec<KafkaOffsetToken>) -> Result<()> {
        if acks.is_empty() {
            return Ok(());
        }
        let highest = Self::highest_per_partition(acks);
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in highest {
            tpl.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset + 1))
                .map_err(|e| Error::Store(e.to_string()))?;
        }
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn flush_nack(&self, _nacks: Vec<KafkaOffsetToken>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highest_per_partition_keeps_max_offset() {
        let tokens = vec![
            KafkaOffsetToken {
                topic: "t".to_owned(),
                partition: 0,
                offset: 5,
            },
            KafkaOffsetToken {
                topic: "t".to_owned(),
                partition: 0,
                offset: 9,
            },
            KafkaOffsetToken {
                topic: "t".to_owned(),
                partition: 0,
                offset: 7,
            },
            KafkaOffsetToken {
                topic: "t".to_owned(),
                partition: 1,
                offset: 1,
            },
        ];
        let map = KafkaFlusher::highest_per_partition(tokens);
        assert_eq!(map.get(&("t".to_owned(), 0)).copied(), Some(9));
        assert_eq!(map.get(&("t".to_owned(), 1)).copied(), Some(1));
    }
}
