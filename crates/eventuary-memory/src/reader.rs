use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::{Mutex, mpsc};

use eventuary_core::io::acker::NoopAcker;
use eventuary_core::io::{Message, Reader};
use eventuary_core::{Event, EventSubscription, Result};

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
    subscription: EventSubscription,
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
        loop {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(event)) if this.subscription.matches(&event) => {
                    this.delivered += 1;
                    return Poll::Ready(Some(Ok(Message::new(event, NoopAcker))));
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Reader for InmemReader {
    type Subscription = EventSubscription;
    type Acker = NoopAcker;
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
    use eventuary_core::io::{BoxReader, ReaderExt};
    use eventuary_core::{OrganizationId, Payload};
    use futures::StreamExt;

    fn subscription() -> EventSubscription {
        EventSubscription::for_organization(OrganizationId::new("org").unwrap())
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
        let reader: BoxReader = InmemReader::new(rx).into_boxed();

        tx.send(ev()).await.unwrap();

        let mut stream = reader.read(subscription()).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        msg.ack().await.unwrap();
    }
}
