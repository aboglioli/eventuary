use crate::error::Result;
use crate::event::Event;
use crate::io::{Filter, Writer};

pub struct FilteredWriter<W, F> {
    inner: W,
    filter: F,
}

impl<W, F> FilteredWriter<W, F> {
    pub fn new(inner: W, filter: F) -> Self {
        Self { inner, filter }
    }
}

impl<W, F> Writer for FilteredWriter<W, F>
where
    W: Writer,
    F: Filter,
{
    async fn write(&self, event: &Event) -> Result<()> {
        if !self.filter.matches(event) {
            return Ok(());
        }
        self.inner.write(event).await
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.iter().all(|event| self.filter.matches(event)) {
            if events.is_empty() {
                return Ok(());
            }
            return self.inner.write_all(events).await;
        }
        let filtered = events
            .iter()
            .filter(|event| self.filter.matches(event))
            .cloned()
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            return Ok(());
        }
        self.inner.write_all(&filtered).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct CapturingWriter {
        events: Arc<Mutex<Vec<Event>>>,
        write_all_calls: Arc<AtomicUsize>,
    }

    impl CapturingWriter {
        fn events(&self) -> Vec<Event> {
            self.events.lock().unwrap().clone()
        }

        fn write_all_calls(&self) -> usize {
            self.write_all_calls.load(Ordering::SeqCst)
        }
    }

    impl Writer for CapturingWriter {
        async fn write(&self, event: &Event) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn write_all(&self, events: &[Event]) -> Result<()> {
            self.write_all_calls.fetch_add(1, Ordering::SeqCst);
            self.events.lock().unwrap().extend(events.iter().cloned());
            Ok(())
        }
    }

    struct TopicFilter(&'static str);

    impl Filter for TopicFilter {
        fn matches(&self, event: &Event) -> bool {
            event.topic().as_str() == self.0
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn filtered_writer_writes_matching_event() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = FilteredWriter::new(inner, TopicFilter("allowed.topic"));

        writer.write(&ev("allowed.topic")).await.unwrap();

        assert_eq!(captured.events().len(), 1);
        assert_eq!(captured.events()[0].topic().as_str(), "allowed.topic");
    }

    #[tokio::test]
    async fn filtered_writer_skips_non_matching_event() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = FilteredWriter::new(inner, TopicFilter("allowed.topic"));

        writer.write(&ev("blocked.topic")).await.unwrap();

        assert!(captured.events().is_empty());
    }

    #[tokio::test]
    async fn filtered_writer_write_all_preserves_batch_for_matches() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = FilteredWriter::new(inner, TopicFilter("allowed.topic"));

        writer
            .write_all(&[
                ev("blocked.one"),
                ev("allowed.topic"),
                ev("blocked.two"),
                ev("allowed.topic"),
            ])
            .await
            .unwrap();

        assert_eq!(captured.write_all_calls(), 1);
        assert_eq!(captured.events().len(), 2);
        assert!(
            captured
                .events()
                .iter()
                .all(|event| event.topic().as_str() == "allowed.topic")
        );
    }

    #[tokio::test]
    async fn filtered_writer_write_all_skips_when_no_match() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = FilteredWriter::new(inner, TopicFilter("allowed.topic"));

        writer
            .write_all(&[ev("blocked.one"), ev("blocked.two")])
            .await
            .unwrap();

        assert_eq!(captured.write_all_calls(), 0);
        assert!(captured.events().is_empty());
    }
}
