use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::io::{Acker, Message};
use crate::payload::Payload;

pub struct SpawnedStream<A: Acker, C, P = Payload> {
    rx: mpsc::Receiver<Result<Message<A, C, P>>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl<A: Acker, C, P> SpawnedStream<A, C, P> {
    pub fn new(
        rx: mpsc::Receiver<Result<Message<A, C, P>>>,
        handle: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            rx,
            handle: Some(handle),
        }
    }

    pub fn from_receiver(rx: mpsc::Receiver<Result<Message<A, C, P>>>) -> Self {
        Self { rx, handle: None }
    }
}

impl<A: Acker, C, P> Stream for SpawnedStream<A, C, P> {
    type Item = Result<Message<A, C, P>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<A: Acker, C, P> Drop for SpawnedStream<A, C, P> {
    fn drop(&mut self) {
        let (_tx, closed) = mpsc::channel::<Result<Message<A, C, P>>>(1);
        let _old_rx = std::mem::replace(&mut self.rx, closed);
        drop(self.handle.take());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;
    use crate::event::Event;
    use crate::io::NoCursor;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    fn ev() -> Event {
        Event::create(
            "org",
            "/x",
            "thing.happened",
            "thing-1",
            Payload::from_string("p"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn poll_next_yields_messages() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(Message::new(ev(), NoopAcker, NoCursor)))
            .await
            .unwrap();
        drop(tx);

        let handle = tokio::spawn(async {});
        let mut stream = SpawnedStream::new(rx, handle);
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.event().topic().as_str(), "thing.happened");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn drop_closes_channel_and_detaches_handle() {
        let (tx, rx) = mpsc::channel::<Result<Message<NoopAcker, NoCursor>>>(1);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        let stream = SpawnedStream::new(rx, handle);
        drop(tx);
        drop(stream);
    }

    #[tokio::test]
    async fn from_receiver_no_abort_on_drop() {
        let (tx, rx) = mpsc::channel::<Result<Message<NoopAcker, NoCursor>>>(1);
        drop(tx);
        let _stream = SpawnedStream::<NoopAcker, NoCursor>::from_receiver(rx);
    }
}
