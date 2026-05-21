use crate::error::Result;
use crate::event::Event;
use crate::io::Writer;

pub struct MapWriter<W, F> {
    inner: W,
    mapper: F,
}

pub struct TryMapWriter<W, F> {
    inner: W,
    mapper: F,
}

impl<W, F> MapWriter<W, F> {
    pub fn new(inner: W, mapper: F) -> Self {
        Self { inner, mapper }
    }
}

impl<W, F> TryMapWriter<W, F> {
    pub fn new(inner: W, mapper: F) -> Self {
        Self { inner, mapper }
    }
}

impl<W, F> Writer for MapWriter<W, F>
where
    W: Writer,
    F: Fn(&Event) -> Event + Send + Sync,
{
    async fn write(&self, event: &Event) -> Result<()> {
        let mapped = (self.mapper)(event);
        self.inner.write(&mapped).await
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        let mapped = events.iter().map(&self.mapper).collect::<Vec<_>>();
        self.inner.write_all(&mapped).await
    }
}

impl<W, F> Writer for TryMapWriter<W, F>
where
    W: Writer,
    F: Fn(&Event) -> Result<Event> + Send + Sync,
{
    async fn write(&self, event: &Event) -> Result<()> {
        let mapped = (self.mapper)(event)?;
        self.inner.write(&mapped).await
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        let mapped = events
            .iter()
            .map(&self.mapper)
            .collect::<Result<Vec<_>>>()?;
        self.inner.write_all(&mapped).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::error::Error;
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

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn map_writer_maps_event_before_write() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = MapWriter::new(inner, |event: &Event| {
            Event::create(
                event.organization().as_str(),
                event.namespace().as_str(),
                "mapped.topic",
                Payload::from_string("mapped"),
            )
            .unwrap()
        });

        writer.write(&ev("source.topic")).await.unwrap();

        let events = captured.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].topic().as_str(), "mapped.topic");
        assert_eq!(events[0].payload().data(), b"mapped");
    }

    #[tokio::test]
    async fn map_writer_write_all_maps_batch_and_uses_inner_write_all() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = MapWriter::new(inner, |event: &Event| {
            Event::create(
                event.organization().as_str(),
                event.namespace().as_str(),
                format!("mapped.{}", event.topic().as_str().replace('.', "_")),
                Payload::from_string("mapped"),
            )
            .unwrap()
        });

        writer.write_all(&[ev("a.one"), ev("b.two")]).await.unwrap();

        let events = captured.events();
        assert_eq!(captured.write_all_calls(), 1);
        assert_eq!(events[0].topic().as_str(), "mapped.a_one");
        assert_eq!(events[1].topic().as_str(), "mapped.b_two");
    }

    #[tokio::test]
    async fn try_map_writer_maps_event_before_write() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = TryMapWriter::new(inner, |event: &Event| {
            Event::create(
                event.organization().as_str(),
                event.namespace().as_str(),
                "try.mapped",
                Payload::from_string("mapped"),
            )
        });

        writer.write(&ev("source.topic")).await.unwrap();

        let events = captured.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].topic().as_str(), "try.mapped");
    }

    #[tokio::test]
    async fn try_map_writer_returns_mapping_error_without_writing() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = TryMapWriter::new(inner, |_event: &Event| -> Result<Event> {
            Err(Error::Config("mapping failed".to_owned()))
        });

        let err = writer.write(&ev("source.topic")).await.unwrap_err();

        assert!(err.to_string().contains("mapping failed"));
        assert!(captured.events().is_empty());
    }

    #[tokio::test]
    async fn try_map_writer_write_all_propagates_first_mapper_error_without_writing() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = TryMapWriter::new(inner, |event: &Event| -> Result<Event> {
            if event.topic().as_str() == "bad" {
                return Err(Error::Config("bad event".to_owned()));
            }
            Event::create(
                event.organization().as_str(),
                event.namespace().as_str(),
                "ok.topic",
                Payload::from_string("p"),
            )
        });

        let err = writer
            .write_all(&[ev("good"), ev("bad"), ev("never.seen")])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("bad event"));
        assert!(captured.events().is_empty());
        assert_eq!(captured.write_all_calls(), 0);
    }

    #[tokio::test]
    async fn map_writer_composes_with_try_map_writer() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let prefix = MapWriter::new(inner, |event: &Event| {
            Event::create(
                event.organization().as_str(),
                event.namespace().as_str(),
                format!("prefix.{}", event.topic().as_str()),
                Payload::from_string("p"),
            )
            .unwrap()
        });
        let suffix = TryMapWriter::new(prefix, |event: &Event| {
            Event::create(
                event.organization().as_str(),
                event.namespace().as_str(),
                format!("{}.suffix", event.topic().as_str()),
                Payload::from_string("p"),
            )
        });

        suffix.write(&ev("core")).await.unwrap();

        let events = captured.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].topic().as_str(), "prefix.core.suffix");
    }
}
