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
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;
    use crate::event::Event;
    use crate::io::NoCursor;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
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
    async fn drop_aborts_handle() {
        let aborted = Arc::new(AtomicBool::new(false));
        let aborted_for_task = Arc::clone(&aborted);

        let (tx, rx) = mpsc::channel::<Result<Message<NoopAcker, NoCursor>>>(1);
        let handle = tokio::spawn(async move {
            let _sentinel = aborted_for_task;
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        let stream = SpawnedStream::new(rx, handle);
        drop(tx);
        drop(stream);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn from_receiver_no_abort_on_drop() {
        let (tx, rx) = mpsc::channel::<Result<Message<NoopAcker, NoCursor>>>(1);
        drop(tx);
        let _stream = SpawnedStream::<NoopAcker, NoCursor>::from_receiver(rx);
    }
}
