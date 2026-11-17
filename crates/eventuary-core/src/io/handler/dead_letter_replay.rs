use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Handler;
use crate::io::handler::retry::{DEAD_LETTER_TOPIC_SUFFIX, DeadLetter};
use crate::payload::Payload;

/// Topic of the marker written once a dead letter has been handled again.
pub const REPLAYED_TOPIC: &str = "dead_letter.replayed";

/// Payload of the marker event; its key is the id of the dead-letter event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterReplayed {
    pub handler_id: String,
}

/// A dead-letter event read back from a log: the event itself and its typed
/// [`DeadLetter`] payload.
///
/// Finding the dead letters that have no marker yet is the backend's job (a
/// query, or a finite read of the log); handling one again and building its
/// marker is the same everywhere, so it is here.
#[derive(Debug, Clone)]
pub struct PendingDeadLetter {
    event: Event,
    letter: DeadLetter,
}

impl PendingDeadLetter {
    pub fn from_event(event: Event) -> Result<Self> {
        if !event.topic().as_str().ends_with(DEAD_LETTER_TOPIC_SUFFIX) {
            return Err(Error::InvalidPayload(format!(
                "topic {} is not a dead-letter topic",
                event.topic()
            )));
        }
        let letter = event.payload().to_json()?;
        Ok(Self { event, letter })
    }

    pub fn event(&self) -> &Event {
        &self.event
    }

    pub fn letter(&self) -> &DeadLetter {
        &self.letter
    }

    pub fn handler_id(&self) -> &str {
        &self.letter.handler_id
    }

    pub fn original_event(&self) -> Result<Event> {
        self.letter.original_event.to_event()
    }

    pub fn replayed_marker(&self) -> Result<Event> {
        let payload = Payload::from_json(&DeadLetterReplayed {
            handler_id: self.letter.handler_id.clone(),
        })?;
        Event::builder(
            self.event.organization().as_str(),
            self.event.namespace().as_str(),
            REPLAYED_TOPIC,
            self.event.id().to_string(),
            payload,
        )?
        .build()
    }

    pub async fn replay<H>(&self, handler: &H) -> Result<Event>
    where
        H: Handler,
    {
        if handler.id() != self.handler_id() {
            return Err(Error::Handler(format!(
                "the dead letter belongs to handler {}, not {}",
                self.handler_id(),
                handler.id()
            )));
        }
        handler.handle(&self.original_event()?).await?;
        self.replayed_marker()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::io::Writer;
    use crate::io::handler::DeadLetterWriter;

    struct Capturing(Arc<Mutex<Vec<Event>>>);

    impl Writer for Capturing {
        async fn write(&self, event: &Event) -> Result<()> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct Counting {
        id: &'static str,
        handled: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<Event>>>,
        fails: bool,
    }

    impl Handler for Counting {
        fn id(&self) -> &str {
            self.id
        }

        async fn handle(&self, event: &Event) -> Result<()> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(event.clone());
            if self.fails {
                return Err(Error::Store("still down".to_owned()));
            }
            Ok(())
        }
    }

    fn counting(
        id: &'static str,
        fails: bool,
    ) -> (Counting, Arc<AtomicUsize>, Arc<Mutex<Vec<Event>>>) {
        let handled = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        (
            Counting {
                id,
                handled: Arc::clone(&handled),
                seen: Arc::clone(&seen),
                fails,
            },
            handled,
            seen,
        )
    }

    fn original() -> Event {
        Event::builder(
            "acme",
            "/orders",
            "order.placed",
            "o-1",
            Payload::from_string("{\"n\":1}"),
        )
        .unwrap()
        .build()
        .unwrap()
    }

    async fn dead_letter_of(original: &Event, handler_id: &str) -> Event {
        let written = Arc::new(Mutex::new(Vec::new()));
        DeadLetterWriter::new(Capturing(Arc::clone(&written)))
            .send(original, handler_id, 3, "down")
            .await
            .unwrap();
        written.lock().unwrap()[0].clone()
    }

    #[tokio::test]
    async fn a_dead_letter_written_by_the_writer_is_read_back_with_its_letter() {
        let source = original();

        let pending =
            PendingDeadLetter::from_event(dead_letter_of(&source, "mailer").await).unwrap();

        assert_eq!(pending.handler_id(), "mailer");
        assert_eq!(pending.letter().attempts, 3);
        assert_eq!(pending.letter().error, "down");
        assert_eq!(pending.letter().original_event_id, *source.id().as_uuid());
        assert_eq!(pending.original_event().unwrap().id(), source.id());
        assert_eq!(pending.event().topic().as_str(), "order.placed.dead_letter");
    }

    #[tokio::test]
    async fn the_marker_names_the_dead_letter_and_its_handler_in_the_same_organization() {
        let pending =
            PendingDeadLetter::from_event(dead_letter_of(&original(), "mailer").await).unwrap();

        let marker = pending.replayed_marker().unwrap();

        assert_eq!(marker.topic().as_str(), REPLAYED_TOPIC);
        assert_eq!(marker.key().as_str(), pending.event().id().to_string());
        assert_eq!(marker.organization().as_str(), "acme");
        assert_eq!(marker.namespace().as_str(), "/orders");
        let payload: DeadLetterReplayed = marker.payload().to_json().unwrap();
        assert_eq!(payload.handler_id, "mailer");
    }

    #[tokio::test]
    async fn replaying_handles_the_original_event_once_and_returns_the_marker() {
        let source = original();
        let pending =
            PendingDeadLetter::from_event(dead_letter_of(&source, "mailer").await).unwrap();
        let (handler, handled, seen) = counting("mailer", false);

        let marker = pending.replay(&handler).await.unwrap();

        assert_eq!(handled.load(Ordering::SeqCst), 1);
        assert_eq!(seen.lock().unwrap()[0].id(), source.id());
        assert_eq!(marker.topic().as_str(), REPLAYED_TOPIC);
    }

    #[tokio::test]
    async fn a_handler_that_fails_again_leaves_no_marker() {
        let pending =
            PendingDeadLetter::from_event(dead_letter_of(&original(), "mailer").await).unwrap();
        let (handler, handled, _) = counting("mailer", true);

        let error = pending.replay(&handler).await.unwrap_err();

        assert!(matches!(error, Error::Store(_)));
        assert_eq!(handled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn another_handler_is_refused_without_running() {
        let pending =
            PendingDeadLetter::from_event(dead_letter_of(&original(), "mailer").await).unwrap();
        let (handler, handled, _) = counting("projector", false);

        let error = pending.replay(&handler).await.unwrap_err();

        assert!(matches!(error, Error::Handler(_)));
        assert_eq!(handled.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn an_event_that_is_not_a_dead_letter_is_refused() {
        let not_a_dead_letter = original();
        let wrong_payload = Event::builder(
            "acme",
            "/orders",
            "order.placed.dead_letter",
            "o-1",
            Payload::from_string("not json"),
        )
        .unwrap()
        .build()
        .unwrap();

        assert!(matches!(
            PendingDeadLetter::from_event(not_a_dead_letter),
            Err(Error::InvalidPayload(_))
        ));
        assert!(PendingDeadLetter::from_event(wrong_payload).is_err());
    }
}
