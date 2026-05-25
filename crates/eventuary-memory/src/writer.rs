use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Payload, Result};

pub struct MemoryWriter<P = Payload> {
    tx: tokio::sync::mpsc::Sender<Event<P>>,
}

impl<P> MemoryWriter<P> {
    pub fn new(tx: tokio::sync::mpsc::Sender<Event<P>>) -> Self {
        Self { tx }
    }
}

impl<P> Writer<P> for MemoryWriter<P>
where
    P: Clone + Send + Sync + 'static,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        self.tx
            .send(event.clone())
            .await
            .map_err(|e| Error::Store(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::Payload;
    use eventuary_core::io::{BoxWriter, Writer, WriterExt};

    fn ev() -> Event {
        Event::create(
            "org",
            "/x",
            "thing.happened",
            "k",
            Payload::from_string("p"),
        )
        .expect("valid event")
    }

    #[tokio::test]
    async fn write_one_event_and_receive_it() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let writer = MemoryWriter::new(tx);
        let event = ev();

        writer.write(&event).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.id(), event.id());
        assert_eq!(received.topic().as_str(), "thing.happened");
        assert_eq!(received.namespace().as_str(), "/x");
        assert_eq!(received.organization().as_str(), "org");
        assert_eq!(received.key().as_str(), "k");
    }

    #[tokio::test]
    async fn write_all_sends_multiple_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(3);
        let writer = MemoryWriter::new(tx);
        let events = vec![ev(), ev(), ev()];

        writer.write_all(&events).await.unwrap();

        for _ in 0..3 {
            assert!(rx.recv().await.is_some());
        }
    }

    #[tokio::test]
    async fn writer_into_boxed_yields_box_writer() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let writer: BoxWriter = MemoryWriter::new(tx).into_boxed();

        writer.write(&ev()).await.unwrap();

        assert!(rx.recv().await.is_some());
    }
}
