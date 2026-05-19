use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Mutex;

use futures::{Stream, StreamExt};

use eventuary_core::io::acker::NoopAcker;
use eventuary_core::io::reader::{
    CheckpointReader, CheckpointScope, CheckpointSubscription, EncodedCursorReader,
    EncodedCursorSubscription, PartitionedCursor, PartitionedReader, PartitionedReaderConfig,
    PartitionedSubscription,
};
use eventuary_core::io::{
    ConsumerGroupId, Cursor, CursorOrder, JsonCursorCodec, Message, Reader, StartFrom,
    StartableSubscription, StreamId,
};
use eventuary_core::{Event, Payload, Result};
use eventuary_memory::checkpoint_store::MemoryCheckpointStore;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
struct SeqCursor(i64);

impl Cursor for SeqCursor {
    fn order_key(&self) -> CursorOrder {
        CursorOrder::from_i64(self.0)
    }
}

#[derive(Debug, Clone, Default)]
struct SeqSubscription {
    start: StartFrom<SeqCursor>,
}

impl StartableSubscription<SeqCursor> for SeqSubscription {
    fn with_start(mut self, start: StartFrom<SeqCursor>) -> Self {
        self.start = start;
        self
    }
}

struct SeqReader {
    events: Mutex<Option<Vec<Event>>>,
}

impl Reader for SeqReader {
    type Subscription = SeqSubscription;
    type Acker = NoopAcker;
    type Cursor = SeqCursor;
    type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, SeqCursor>>> + Send>>;

    async fn read(&self, _: SeqSubscription) -> Result<Self::Stream> {
        let events = self.events.lock().unwrap().take().unwrap();
        let iter = events
            .into_iter()
            .enumerate()
            .map(|(i, e)| Ok(Message::new(e, NoopAcker, SeqCursor(i as i64 + 1))));
        Ok(Box::pin(futures::stream::iter(iter)))
    }
}

fn ev(key: &str) -> Event {
    Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
        .unwrap()
        .key(key)
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn partitioned_then_encoded_then_checkpointed_roundtrip() {
    let events: Vec<Event> = (0..8).map(|i| ev(&format!("k{i}"))).collect();
    let inner = SeqReader {
        events: Mutex::new(Some(events)),
    };
    let partitioned = PartitionedReader::source(
        inner,
        PartitionedReaderConfig {
            partition_count: NonZeroU16::new(4).unwrap(),
            ..PartitionedReaderConfig::default()
        },
    );
    let codec =
        JsonCursorCodec::<PartitionedCursor<SeqCursor>>::new("eventuary.test.partitioned_seq.v1")
            .unwrap();
    let encoded = EncodedCursorReader::new(partitioned, codec);
    let store = MemoryCheckpointStore::new();
    let checkpointed = CheckpointReader::new(encoded, store);

    let scope = CheckpointScope::new(
        ConsumerGroupId::new("g").unwrap(),
        StreamId::new("s").unwrap(),
    );
    let mut stream = checkpointed
        .read(CheckpointSubscription::new(
            EncodedCursorSubscription::new(
                PartitionedSubscription::new(SeqSubscription::default()),
            ),
            scope,
        ))
        .await
        .unwrap();

    let mut delivered = 0usize;
    while delivered < 8 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        msg.ack().await.unwrap();
        delivered += 1;
    }
    assert_eq!(delivered, 8);
}
