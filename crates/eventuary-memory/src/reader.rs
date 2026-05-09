use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::{Mutex, mpsc};

use eventuary::io::acker::NoopAcker;
use eventuary::io::{Message, Reader};
use eventuary::{Event, Result};

pub struct InmemReader {
    rx: Arc<Mutex<mpsc::Receiver<Event>>>,
}

impl InmemReader {
    pub fn new(rx: mpsc::Receiver<Event>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

pub struct InmemStream {
    rx: Arc<Mutex<mpsc::Receiver<Event>>>,
}

impl Stream for InmemStream {
    type Item = Result<Message<NoopAcker>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut rx = self.rx.try_lock().expect("inmem stream lock");
        rx.poll_recv(cx)
            .map(|opt| opt.map(|event| Ok(Message::new(event, NoopAcker))))
    }
}

impl Reader for InmemReader {
    type Acker = NoopAcker;
    type Stream = InmemStream;

    async fn read(&self) -> Result<Self::Stream> {
        Ok(InmemStream {
            rx: Arc::clone(&self.rx),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary::Payload;
    use eventuary::io::{BoxReader, ReaderExt};
    use futures::StreamExt;

    fn ev() -> Event {
        Event::create(
            "org",
            "/x",
            "thing.happened",
            "k",
            Payload::from_string("p"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn reader_reads_one_event() {
        let (tx, rx) = mpsc::channel(1);
        let reader = InmemReader::new(rx);

        let mut stream = reader.read().await.unwrap();
        tx.send(ev()).await.unwrap();

        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_reads_multiple_events() {
        let (tx, rx) = mpsc::channel(2);
        let reader = InmemReader::new(rx);

        let mut stream = reader.read().await.unwrap();

        tokio::spawn(async move {
            tx.send(ev()).await.unwrap();
            tx.send(ev()).await.unwrap();
        });

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();

        let msg2 = stream.next().await.unwrap().unwrap();
        msg2.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_into_boxed_yields_box_reader() {
        let (tx, rx) = mpsc::channel(1);
        let reader: BoxReader = InmemReader::new(rx).into_boxed();

        tx.send(ev()).await.unwrap();

        let mut stream = reader.read().await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }
}
