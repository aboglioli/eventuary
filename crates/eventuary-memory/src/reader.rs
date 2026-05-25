use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc;

use eventuary_core::io::acker::NoopAcker;
use eventuary_core::io::{Message, NoCursor, Reader};
use eventuary_core::{Error, Event, Payload, Result, StartFrom, StartableSubscription};

#[derive(Debug, Clone, Default)]
pub struct MemorySubscription {
    pub limit: Option<usize>,
}

impl StartableSubscription<NoCursor> for MemorySubscription {
    fn with_start(self, _: StartFrom<NoCursor>) -> Self {
        self
    }
}

pub struct MemoryReader<P = Payload> {
    rx: Arc<Mutex<mpsc::Receiver<Event<P>>>>,
}

impl<P> MemoryReader<P> {
    pub fn new(rx: mpsc::Receiver<Event<P>>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

pub struct InmemStream<P = Payload> {
    rx: Arc<Mutex<mpsc::Receiver<Event<P>>>>,
    subscription: MemorySubscription,
    delivered: usize,
}

impl<P: Send + 'static> Stream for InmemStream<P> {
    type Item = Result<Message<NoopAcker, NoCursor, P>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(limit) = this.subscription.limit
            && this.delivered >= limit
        {
            return Poll::Ready(None);
        }
        let Ok(mut rx) = this.rx.lock() else {
            return Poll::Ready(Some(Err(Error::Store(
                "memory reader lock poisoned".to_owned(),
            ))));
        };
        match rx.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                this.delivered += 1;
                Poll::Ready(Some(Ok(Message::new(event, NoopAcker, NoCursor))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P: Send + 'static> Reader<P> for MemoryReader<P> {
    type Subscription = MemorySubscription;
    type Acker = NoopAcker;
    type Cursor = NoCursor;
    type Stream = InmemStream<P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        Ok(InmemStream {
            rx: Arc::clone(&self.rx),
            subscription,
            delivered: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::Payload;
    use eventuary_core::io::{BoxReader, ReaderExt, Writer};
    use futures::StreamExt;

    fn subscription() -> MemorySubscription {
        MemorySubscription { limit: None }
    }

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
    async fn reader_reads_one_event() {
        let (tx, rx) = mpsc::channel(1);
        let reader = MemoryReader::new(rx);

        let mut stream = reader.read(subscription()).await.unwrap();
        tx.send(ev()).await.unwrap();

        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_reads_multiple_events() {
        let (tx, rx) = mpsc::channel(2);
        let reader = MemoryReader::new(rx);

        let mut stream = reader.read(subscription()).await.unwrap();

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
        let reader: BoxReader<MemorySubscription, NoCursor> = MemoryReader::new(rx).into_boxed();

        tx.send(ev()).await.unwrap();

        let mut stream = reader.read(subscription()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_does_not_filter_by_event_predicate() {
        let (tx, rx) = mpsc::channel(2);
        let reader = MemoryReader::new(rx);

        let mut stream = reader.read(subscription()).await.unwrap();

        let a = Event::create(
            "org-a",
            "/x",
            "thing.happened",
            "k",
            Payload::from_string("p"),
        )
        .expect("valid event");
        let b = Event::create(
            "org-b",
            "/y",
            "other.happened",
            "k",
            Payload::from_string("p"),
        )
        .expect("valid event");

        tx.send(a).await.unwrap();
        tx.send(b).await.unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        assert_eq!(msg1.event().organization().as_str(), "org-a");
        msg1.ack().await.unwrap();

        let msg2 = stream.next().await.unwrap().unwrap();
        assert_eq!(msg2.event().organization().as_str(), "org-b");
        msg2.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_honors_limit() {
        let (tx, rx) = mpsc::channel(3);
        let reader = MemoryReader::new(rx);

        let mut stream = reader
            .read(MemorySubscription { limit: Some(2) })
            .await
            .unwrap();

        tx.send(ev()).await.unwrap();
        tx.send(ev()).await.unwrap();
        tx.send(ev()).await.unwrap();

        let msg1 = stream.next().await.unwrap().unwrap();
        msg1.ack().await.unwrap();
        let msg2 = stream.next().await.unwrap().unwrap();
        msg2.ack().await.unwrap();

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn memory_backend_processes_typed_events_without_payload_serialization() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct UserUpdated {
            user_id: String,
        }

        let (tx, rx) = mpsc::channel(1);
        let writer = crate::writer::MemoryWriter::<UserUpdated>::new(tx);
        let reader = MemoryReader::<UserUpdated>::new(rx);

        writer
            .write(
                &Event::create(
                    "org",
                    "/users",
                    "user.updated",
                    "u-1",
                    UserUpdated {
                        user_id: "u-1".to_owned(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut stream = reader
            .read(MemorySubscription { limit: None })
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().payload().user_id, "u-1");
    }
}
