use crate::error::Result;
use crate::event::Event;
use crate::io::Acker;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NoCursor;

impl crate::partition::CursorPartition for NoCursor {
    fn partition(&self) -> Option<crate::partition::LogicalPartition> {
        None
    }
}

impl crate::partition::CommitCursor for NoCursor {
    type Commit = NoCursor;
    fn commit_cursor(&self) -> Self::Commit {
        NoCursor
    }
}

pub struct Message<A: Acker, C = NoCursor> {
    event: Event,
    acker: A,
    cursor: C,
}

impl<A: Acker, C> Message<A, C> {
    pub fn new(event: Event, acker: A, cursor: C) -> Self {
        Self {
            event,
            acker,
            cursor,
        }
    }

    pub fn event(&self) -> &Event {
        &self.event
    }

    pub fn cursor(&self) -> &C {
        &self.cursor
    }

    pub fn into_event(self) -> Event {
        self.event
    }

    pub async fn ack(&self) -> Result<()> {
        self.acker.ack().await
    }

    pub async fn nack(&self) -> Result<()> {
        self.acker.nack().await
    }

    pub fn acker(&self) -> &A {
        &self.acker
    }

    pub fn into_parts(self) -> (Event, A, C) {
        (self.event, self.acker, self.cursor)
    }

    pub fn map_acker<B, F>(self, f: F) -> Message<B, C>
    where
        B: Acker,
        F: FnOnce(A) -> B,
    {
        Message {
            event: self.event,
            acker: f(self.acker),
            cursor: self.cursor,
        }
    }

    pub fn map_cursor<D, F>(self, f: F) -> Message<A, D>
    where
        F: FnOnce(C) -> D,
    {
        Message {
            event: self.event,
            acker: self.acker,
            cursor: f(self.cursor),
        }
    }

    pub fn map_event<F>(self, f: F) -> Self
    where
        F: FnOnce(Event) -> Event,
    {
        Message {
            event: f(self.event),
            acker: self.acker,
            cursor: self.cursor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(i64);

    struct OtherAcker;

    impl Acker for OtherAcker {
        async fn ack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[test]
    fn map_acker_swaps_acker_keeps_event() {
        let msg = Message::new(ev(), NoopAcker, NoCursor);
        let topic_before = msg.event().topic().as_str().to_owned();
        let mapped: Message<OtherAcker, NoCursor> = msg.map_acker(|_| OtherAcker);
        assert_eq!(mapped.event().topic().as_str(), topic_before);
    }

    #[test]
    fn map_event_swaps_event_keeps_acker() {
        let msg = Message::new(ev(), NoopAcker, NoCursor);
        let mapped = msg.map_event(|e| {
            Event::create("org", "/y", e.topic().as_str(), Payload::from_string("p2")).unwrap()
        });
        assert_eq!(mapped.event().namespace().as_str(), "/y");
    }

    #[test]
    fn message_exposes_cursor() {
        let msg = Message::new(ev(), NoopAcker, TestCursor(7));
        assert_eq!(*msg.cursor(), TestCursor(7));
    }

    #[test]
    fn map_acker_keeps_cursor() {
        let msg = Message::new(ev(), NoopAcker, TestCursor(7));
        let mapped: Message<OtherAcker, TestCursor> = msg.map_acker(|_| OtherAcker);
        assert_eq!(*mapped.cursor(), TestCursor(7));
    }

    #[test]
    fn map_cursor_keeps_event_and_acker() {
        let msg = Message::new(ev(), NoopAcker, TestCursor(7));
        let topic = msg.event().topic().clone();

        let mapped = msg.map_cursor(|cursor| TestCursor(cursor.0 + 1));

        assert_eq!(mapped.event().topic(), &topic);
        assert_eq!(*mapped.cursor(), TestCursor(8));
    }

    #[test]
    fn into_parts_returns_cursor() {
        let msg = Message::new(ev(), NoopAcker, TestCursor(7));
        let (_event, _acker, cursor) = msg.into_parts();
        assert_eq!(cursor, TestCursor(7));
    }
}
