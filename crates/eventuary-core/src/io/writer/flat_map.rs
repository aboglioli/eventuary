use std::marker::PhantomData;

use crate::error::Result;
use crate::event::Event;
use crate::io::Writer;
use crate::payload::Payload;

pub struct FlatMapWriter<W, F, P = Payload, Q = Payload> {
    inner: W,
    mapper: F,
    _payload: PhantomData<fn(P) -> Q>,
}

pub struct TryFlatMapWriter<W, F, P = Payload, Q = Payload> {
    inner: W,
    mapper: F,
    _payload: PhantomData<fn(P) -> Q>,
}

impl<W, F, P, Q> FlatMapWriter<W, F, P, Q> {
    pub fn new(inner: W, mapper: F) -> Self {
        Self {
            inner,
            mapper,
            _payload: PhantomData,
        }
    }
}

impl<W, F, P, Q> TryFlatMapWriter<W, F, P, Q> {
    pub fn new(inner: W, mapper: F) -> Self {
        Self {
            inner,
            mapper,
            _payload: PhantomData,
        }
    }
}

impl<W, F, P, Q> Writer<P> for FlatMapWriter<W, F, P, Q>
where
    W: Writer<Q>,
    F: Fn(&Event<P>) -> Vec<Event<Q>> + Send + Sync,
    P: Send + Sync,
    Q: Send + Sync,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        let events = (self.mapper)(event);
        self.inner.write_all(&events).await
    }

    async fn write_all(&self, events: &[Event<P>]) -> Result<()> {
        let mapped = events
            .iter()
            .flat_map(|event| (self.mapper)(event))
            .collect::<Vec<_>>();
        self.inner.write_all(&mapped).await
    }
}

impl<W, F, P, Q> Writer<P> for TryFlatMapWriter<W, F, P, Q>
where
    W: Writer<Q>,
    F: Fn(&Event<P>) -> Result<Vec<Event<Q>>> + Send + Sync,
    P: Send + Sync,
    Q: Send + Sync,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        let events = (self.mapper)(event)?;
        self.inner.write_all(&events).await
    }

    async fn write_all(&self, events: &[Event<P>]) -> Result<()> {
        let mut mapped = Vec::new();
        for event in events {
            mapped.extend((self.mapper)(event)?);
        }
        self.inner.write_all(&mapped).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::error::Error;
    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct RecordingWriter {
        batches: Arc<Mutex<Vec<Vec<Event>>>>,
    }

    impl RecordingWriter {
        fn batches(&self) -> Vec<Vec<Event>> {
            self.batches.lock().unwrap().clone()
        }

        fn total_events(&self) -> usize {
            self.batches.lock().unwrap().iter().map(Vec::len).sum()
        }
    }

    impl Writer for RecordingWriter {
        async fn write(&self, event: &Event) -> Result<()> {
            self.batches.lock().unwrap().push(vec![event.clone()]);
            Ok(())
        }

        async fn write_all(&self, events: &[Event]) -> Result<()> {
            self.batches.lock().unwrap().push(events.to_vec());
            Ok(())
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, "thing-1", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn flat_map_writer_writes_all_derived_events() {
        let inner = RecordingWriter::default();
        let captured = inner.clone();
        let writer = FlatMapWriter::new(inner, |event: &Event| {
            vec![
                Event::create(
                    event.organization().as_str(),
                    event.namespace().as_str(),
                    format!("{}.first", event.topic().as_str()),
                    event.key().as_str(),
                    Payload::from_string("p"),
                )
                .unwrap(),
                Event::create(
                    event.organization().as_str(),
                    event.namespace().as_str(),
                    format!("{}.second", event.topic().as_str()),
                    event.key().as_str(),
                    Payload::from_string("p"),
                )
                .unwrap(),
            ]
        });

        writer.write(&ev("source")).await.unwrap();

        let batches = captured.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[0][0].topic().as_str(), "source.first");
        assert_eq!(batches[0][1].topic().as_str(), "source.second");
    }

    #[tokio::test]
    async fn flat_map_writer_allows_zero_derived_events() {
        let inner = RecordingWriter::default();
        let captured = inner.clone();
        let writer = FlatMapWriter::new(inner, |_event: &Event| Vec::<Event>::new());

        writer.write(&ev("source")).await.unwrap();

        let batches = captured.batches();
        assert_eq!(batches.len(), 1);
        assert!(batches[0].is_empty());
        assert_eq!(captured.total_events(), 0);
    }

    #[tokio::test]
    async fn try_flat_map_writer_returns_mapper_error_without_writing() {
        let inner = RecordingWriter::default();
        let captured = inner.clone();
        let writer = TryFlatMapWriter::new(inner, |_event: &Event| -> Result<Vec<Event>> {
            Err(Error::Config("nope".to_owned()))
        });

        let err = writer.write(&ev("source")).await.unwrap_err();

        assert!(err.to_string().contains("nope"));
        assert!(captured.batches().is_empty());
    }

    #[tokio::test]
    async fn try_flat_map_writer_write_all_flattens_inputs_into_one_batch() {
        let inner = RecordingWriter::default();
        let captured = inner.clone();
        let writer = TryFlatMapWriter::new(inner, |event: &Event| -> Result<Vec<Event>> {
            Ok(vec![
                Event::create(
                    event.organization().as_str(),
                    event.namespace().as_str(),
                    format!("{}.a", event.topic().as_str()),
                    event.key().as_str(),
                    Payload::from_string("p"),
                )?,
                Event::create(
                    event.organization().as_str(),
                    event.namespace().as_str(),
                    format!("{}.b", event.topic().as_str()),
                    event.key().as_str(),
                    Payload::from_string("p"),
                )?,
            ])
        });

        writer.write_all(&[ev("one"), ev("two")]).await.unwrap();

        let batches = captured.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 4);
        assert_eq!(batches[0][0].topic().as_str(), "one.a");
        assert_eq!(batches[0][1].topic().as_str(), "one.b");
        assert_eq!(batches[0][2].topic().as_str(), "two.a");
        assert_eq!(batches[0][3].topic().as_str(), "two.b");
    }
}
