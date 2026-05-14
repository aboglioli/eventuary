use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::io::acker::{
    AckBuffer, AckBufferConfig, AckCmd, AckShutdown, BatchFlusher, BatchedAcker,
};
use crate::io::{Message, NoCursor};

pub struct BatchedStream<T: Clone + Send + Sync + 'static, C = NoCursor> {
    rx: mpsc::Receiver<Result<Message<BatchedAcker<T>, C>>>,
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
    ack: Arc<dyn AckShutdown>,
}

impl<T: Clone + Send + Sync + 'static, C> Stream for BatchedStream<T, C> {
    type Item = Result<Message<BatchedAcker<T>, C>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<T: Clone + Send + Sync + 'static, C> Drop for BatchedStream<T, C> {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        let ack = Arc::clone(&self.ack);
        tokio::spawn(async move {
            let _ = ack.shutdown().await;
        });
    }
}

pub fn batched_source<T: Clone + Send + Sync + 'static, C: Send + 'static>(
    flusher: impl BatchFlusher<Token = T> + 'static,
    ack_config: AckBufferConfig,
    channel_capacity: usize,
    source_loop: impl FnOnce(
        mpsc::Sender<Result<Message<BatchedAcker<T>, C>>>,
        mpsc::Sender<AckCmd<T>>,
        CancellationToken,
    ) -> BoxFuture<'static, ()>
    + Send
    + 'static,
) -> BatchedStream<T, C> {
    let ack_buffer = Arc::new(AckBuffer::spawn(flusher, ack_config));
    let ack: Arc<dyn AckShutdown> = Arc::clone(&ack_buffer) as Arc<dyn AckShutdown>;
    let tx_ack = ack_buffer.sender();
    let (tx, rx) = mpsc::channel(channel_capacity);
    let cancel = CancellationToken::new();

    let cancel_for_task = cancel.clone();
    let tx_for_task = tx.clone();
    let handle = tokio::spawn(async move {
        source_loop(tx_for_task, tx_ack, cancel_for_task).await;
    });

    BatchedStream {
        rx,
        cancel,
        handle: Some(handle),
        ack,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;
    use crate::event::Event;
    use crate::payload::Payload;

    fn ev(key: &str) -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .unwrap()
    }

    struct CountingFlusher {
        flushed: Arc<AtomicUsize>,
    }

    impl BatchFlusher for CountingFlusher {
        type Token = String;

        async fn flush(&self, _tokens: Vec<Self::Token>) -> Result<()> {
            self.flushed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn flush_nack(&self, _tokens: Vec<Self::Token>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn batched_source_delivers_messages() {
        let flusher_count = Arc::new(AtomicUsize::new(0));
        let flusher = CountingFlusher {
            flushed: Arc::clone(&flusher_count),
        };

        let mut stream = batched_source(
            flusher,
            AckBufferConfig::default(),
            64,
            |tx, tx_ack, _cancel| {
                Box::pin(async move {
                    for i in 0..3 {
                        let acker = BatchedAcker::new(format!("token{i}"), tx_ack.clone());
                        let msg = Message::new(ev(&format!("k{i}")), acker, NoCursor);
                        let _ = tx.send(Ok(msg)).await;
                    }
                })
            },
        );

        let mut received = 0usize;
        while let Some(Ok(msg)) = stream.next().await {
            msg.ack().await.unwrap();
            received += 1;
        }
        assert_eq!(received, 3);
        drop(stream);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(flusher_count.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn batched_source_cancels_on_drop() {
        let started = Arc::new(AtomicUsize::new(0));
        let started_clone = Arc::clone(&started);

        let flusher = CountingFlusher {
            flushed: Arc::new(AtomicUsize::new(0)),
        };

        let stream = batched_source(
            flusher,
            AckBufferConfig::default(),
            1,
            |tx, tx_ack, cancel| {
                Box::pin(async move {
                    started_clone.store(1, Ordering::SeqCst);
                    cancel.cancelled().await;
                    let acker = BatchedAcker::new("x".to_owned(), tx_ack);
                    let _ = tx.send(Ok(Message::new(ev("k0"), acker, NoCursor))).await;
                })
            },
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(started.load(Ordering::SeqCst), 1);
        drop(stream);
    }
}
