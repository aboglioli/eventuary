use std::fmt;

use crate::error::Result;
use crate::event::Event;
use crate::io::Acker;
use crate::io::acker::NackContext;
use crate::payload::Payload;

pub struct Message<A: Acker, C, P = Payload> {
    event: Event<P>,
    acker: A,
    cursor: C,
}

impl<A: Acker + fmt::Debug, C: fmt::Debug, P: fmt::Debug> fmt::Debug for Message<A, C, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("event", &self.event)
            .field("acker", &self.acker)
            .field("cursor", &self.cursor)
            .finish()
    }
}

impl<A: Acker + Clone, C: Clone, P: Clone> Clone for Message<A, C, P> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            acker: self.acker.clone(),
            cursor: self.cursor.clone(),
        }
    }
}

impl<A: Acker, C, P> Message<A, C, P> {
    pub fn new(event: Event<P>, acker: A, cursor: C) -> Self {
        Self {
            event,
            acker,
            cursor,
        }
    }

    pub fn event(&self) -> &Event<P> {
        &self.event
    }

    pub fn cursor(&self) -> &C {
        &self.cursor
    }

    pub fn into_event(self) -> Event<P> {
        self.event
    }

    pub async fn ack(&self) -> Result<()> {
        self.acker.ack().await
    }

    pub async fn nack(&self) -> Result<()> {
        self.acker.nack().await
    }

    pub async fn nack_with(&self, context: NackContext) -> Result<()> {
        self.acker.nack_with(context).await
    }

    pub fn acker(&self) -> &A {
        &self.acker
    }

    pub fn into_parts(self) -> (Event<P>, A, C) {
        (self.event, self.acker, self.cursor)
    }

    pub fn map_acker<B, F>(self, f: F) -> Message<B, C, P>
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

    pub fn map_cursor<D, F>(self, f: F) -> Message<A, D, P>
    where
        F: FnOnce(C) -> D,
    {
        Message {
            event: self.event,
            acker: self.acker,
            cursor: f(self.cursor),
        }
    }

    pub fn map_event<Q, F>(self, f: F) -> Message<A, C, Q>
    where
        F: FnOnce(Event<P>) -> Event<Q>,
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

    use std::sync::{Arc, Mutex};

    use crate::io::NoCursor;
    use crate::io::acker::{NackReason, NoopAcker};

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
        Event::create(
            "org",
            "/x",
            "thing.happened",
            "thing-1",
            super::Payload::from_string("p"),
        )
        .unwrap()
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
            Event::create(
                "org",
                "/y",
                e.topic().as_str(),
                "thing-1",
                super::Payload::from_string("p2"),
            )
            .unwrap()
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

    #[test]
    fn message_holds_typed_event() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct UserUpdated {
            user_id: String,
        }

        let event: Event<UserUpdated> = Event::create(
            "org",
            "/users",
            "user.updated",
            "thing-1",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap();

        let msg: Message<NoopAcker, NoCursor, UserUpdated> =
            Message::new(event, NoopAcker, NoCursor);
        assert_eq!(msg.event().payload().user_id, "u-1");
    }

    #[test]
    fn map_event_changes_message_payload_type() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct UserUpdated {
            user_id: String,
        }

        let event: Event<UserUpdated> = Event::create(
            "org",
            "/users",
            "user.updated",
            "thing-1",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap();

        let msg: Message<NoopAcker, NoCursor, UserUpdated> =
            Message::new(event, NoopAcker, NoCursor);
        let mapped: Message<NoopAcker, NoCursor, String> =
            msg.map_event(|event| event.map_payload(|p| p.user_id));

        assert_eq!(mapped.event().payload(), "u-1");
    }

    struct ContextAcker {
        captured: Arc<Mutex<Vec<NackContext>>>,
    }

    impl Acker for ContextAcker {
        async fn ack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack_with(&self, context: NackContext) -> Result<()> {
            self.captured.lock().unwrap().push(context);
            Ok(())
        }
    }

    #[tokio::test]
    async fn nack_with_forwards_context_to_acker() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let acker = ContextAcker {
            captured: Arc::clone(&captured),
        };
        let msg = Message::new(ev(), acker, NoCursor);
        let context = NackContext::processing_rejected("bad payload").unwrap();
        msg.nack_with(context).await.unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].reason(), NackReason::ProcessingRejected);
        assert_eq!(captured[0].context().message(), "bad payload");
    }
}
