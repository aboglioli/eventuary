use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::{Mutex, mpsc};

use eventuary_core::io::acker::NoopAcker;
use eventuary_core::io::checkpoint::{CheckpointResumableSubscription, CheckpointResume};
use eventuary_core::io::{Message, NoCursor, Reader};
use eventuary_core::{Event, Result};

#[derive(Debug, Clone, Default)]
pub struct MemorySubscription {
    pub limit: Option<usize>,
}

impl CheckpointResumableSubscription<NoCursor> for MemorySubscription {
    fn with_checkpoint_resume(self, _: CheckpointResume<NoCursor>) -> Self {
        self
    }
}

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
    subscription: MemorySubscription,
    delivered: usize,
}

impl Stream for InmemStream {
    type Item = Result<Message<NoopAcker>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(limit) = this.subscription.limit
            && this.delivered >= limit
        {
            return Poll::Ready(None);
        }
        let mut rx = this.rx.try_lock().expect("inmem stream lock");
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

impl Reader for InmemReader {
    type Subscription = MemorySubscription;
    type Acker = NoopAcker;
    type Cursor = NoCursor;
    type Stream = InmemStream;

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
    use eventuary_core::io::{BoxReader, ReaderExt};
    use futures::StreamExt;

    fn subscription() -> MemorySubscription {
        MemorySubscription { limit: None }
    }

    fn ev() -> Event {
        Event::builder("org", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .build()
            .expect("valid event")
    }

    #[tokio::test]
    async fn reader_reads_one_event() {
        let (tx, rx) = mpsc::channel(1);
        let reader = InmemReader::new(rx);

        let mut stream = reader.read(subscription()).await.unwrap();
        tx.send(ev()).await.unwrap();

        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_reads_multiple_events() {
        let (tx, rx) = mpsc::channel(2);
        let reader = InmemReader::new(rx);

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
        let reader: BoxReader<MemorySubscription, NoCursor> = InmemReader::new(rx).into_boxed();

        tx.send(ev()).await.unwrap();

        let mut stream = reader.read(subscription()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn reader_does_not_filter_by_event_predicate() {
        let (tx, rx) = mpsc::channel(2);
        let reader = InmemReader::new(rx);

        let mut stream = reader.read(subscription()).await.unwrap();

        let a = Event::builder("org-a", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .build()
            .expect("valid event");
        let b = Event::builder("org-b", "/y", "other.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .build()
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

    #[test]
    fn memory_subscription_accepts_checkpoint_resume_without_changing_fields() {
        use eventuary_core::StartFrom;
        use eventuary_core::io::NoCursor;
        use eventuary_core::io::checkpoint::{
            CheckpointResumableSubscription, CheckpointResume, CheckpointResumePoint,
        };

        let subscription = MemorySubscription { limit: Some(5) };
        let resume = CheckpointResume::new(
            StartFrom::After(NoCursor),
            vec![CheckpointResumePoint::new(None, NoCursor)],
        );

        let resumed = subscription.clone().with_checkpoint_resume(resume);

        assert_eq!(resumed.limit, subscription.limit);
    }

    #[tokio::test]
    async fn reader_honors_limit() {
        let (tx, rx) = mpsc::channel(3);
        let reader = InmemReader::new(rx);

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
}
