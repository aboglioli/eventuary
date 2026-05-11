use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Payload;
use crate::error::Result;
use crate::event::EventId;

pub trait Snapshot: Sized {
    fn take_snapshot(&self) -> Result<Payload>;
    fn from_snapshot(payload: &Payload) -> Result<Self>;
    fn snapshot_event_id(&self) -> EventId;
}

pub trait SnapshotEventId {
    fn snapshot_event_id(&self) -> EventId;
}

impl<T: Serialize + DeserializeOwned + SnapshotEventId> Snapshot for T {
    fn take_snapshot(&self) -> Result<Payload> {
        Payload::from_json(self)
    }

    fn from_snapshot(payload: &Payload) -> Result<Self> {
        payload.to_json()
    }

    fn snapshot_event_id(&self) -> EventId {
        SnapshotEventId::snapshot_event_id(self)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct Counter {
        last_event: String,
        value: u64,
    }

    impl SnapshotEventId for Counter {
        fn snapshot_event_id(&self) -> EventId {
            EventId::from_str(&self.last_event).unwrap()
        }
    }

    #[test]
    fn snapshot_roundtrip() {
        let event_id = EventId::new();
        let counter = Counter {
            last_event: event_id.to_string(),
            value: 42,
        };
        let payload = counter.take_snapshot().unwrap();
        let restored: Counter = Counter::from_snapshot(&payload).unwrap();
        assert_eq!(restored, counter);
        assert_eq!(Snapshot::snapshot_event_id(&restored), event_id);
    }
}
