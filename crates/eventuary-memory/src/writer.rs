use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result};

pub struct InmemWriter {
    tx: tokio::sync::mpsc::Sender<Event>,
}

impl InmemWriter {
    pub fn new(tx: tokio::sync::mpsc::Sender<Event>) -> Self {
        Self { tx }
    }
}

impl Writer for InmemWriter {
    async fn write(&self, event: &Event) -> Result<()> {
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
    use eventuary_core::io::{BoxWriter, WriterExt};

    fn ev() -> Event {
        Event::builder("org", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .build()
            .expect("valid event")
    }

    #[tokio::test]
    async fn write_one_event_and_receive_it() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let writer = InmemWriter::new(tx);
        let event = ev();

        writer.write(&event).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.id(), event.id());
        assert_eq!(received.topic().as_str(), "thing.happened");
        assert_eq!(received.namespace().as_str(), "/x");
        assert_eq!(received.organization().as_str(), "org");
        assert_eq!(received.key().expect("event has key").as_str(), "k");
    }

    #[tokio::test]
    async fn write_all_sends_multiple_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(3);
        let writer = InmemWriter::new(tx);
        let events = vec![ev(), ev(), ev()];

        writer.write_all(&events).await.unwrap();

        for _ in 0..3 {
            assert!(rx.recv().await.is_some());
        }
    }

    #[tokio::test]
    async fn writer_into_boxed_yields_box_writer() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let writer: BoxWriter = InmemWriter::new(tx).into_boxed();

        writer.write(&ev()).await.unwrap();

        assert!(rx.recv().await.is_some());
    }
}
