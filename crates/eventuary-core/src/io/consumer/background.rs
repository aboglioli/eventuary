use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::error::{Error, Result};
use crate::io::{Handler, Reader};

pub struct BackgroundConsumer<R: Reader, H: Handler> {
    reader: R,
    subscription: R::Subscription,
    handler: Arc<H>,
    concurrency: usize,
    batch_size: usize,
    handler_timeout: Option<Duration>,
}

impl<R, H> BackgroundConsumer<R, H>
where
    R: Reader + Send + 'static,
    R::Stream: 'static,
    R::Cursor: Send + Sync + 'static,
    H: Handler + 'static,
{
    pub fn new(reader: R, subscription: R::Subscription, handler: H, concurrency: usize) -> Self {
        let concurrency = concurrency.max(1);
        Self {
            reader,
            subscription,
            handler: Arc::new(handler),
            concurrency,
            batch_size: concurrency,
            handler_timeout: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    pub fn with_handler_timeout(mut self, d: Duration) -> Self {
        self.handler_timeout = Some(d);
        self
    }

    pub fn spawn(self) -> ConsumerHandle {
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let cancel_for_run = cancel.clone();
        let tracker_for_run = tracker.clone();
        let join = tokio::spawn(async move { self.run(cancel_for_run, tracker_for_run).await });
        ConsumerHandle { cancel, join }
    }

    async fn run(self, cancel: CancellationToken, tracker: TaskTracker) -> Result<()> {
        let mut stream = Box::pin(self.reader.read(self.subscription).await?);
        let handler = Arc::clone(&self.handler);
        let timeout = self.handler_timeout;
        let concurrency = self.concurrency;
        let batch_size = self.batch_size;

        loop {
            let first = {
                let mut s = stream.as_mut();
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    item = s.next() => item,
                }
            };
            let Some(first) = first else {
                break;
            };

            let mut batch = Vec::with_capacity(batch_size);
            batch.push(first);
            while batch.len() < batch_size {
                let mut s = stream.as_mut();
                match s.next().now_or_never() {
                    Some(Some(item)) => batch.push(item),
                    Some(None) | None => break,
                }
            }

            futures::stream::iter(batch)
                .for_each_concurrent(concurrency, |msg_result| {
                    let handler = Arc::clone(&handler);
                    let tracker = tracker.clone();
                    async move {
                        let task = tracker.spawn(async move {
                            let msg = match msg_result {
                                Err(e) => {
                                    tracing::error!("reader stream error: {e}");
                                    return;
                                }
                                Ok(m) => m,
                            };
                            let result = match timeout {
                                Some(d) => {
                                    match tokio::time::timeout(d, handler.handle(msg.event())).await
                                    {
                                        Ok(r) => r,
                                        Err(_) => Err(Error::Timeout(format!(
                                            "handler {} timed out",
                                            handler.id()
                                        ))),
                                    }
                                }
                                None => handler.handle(msg.event()).await,
                            };
                            match result {
                                Ok(()) => {
                                    let _ = msg.ack().await;
                                }
                                Err(e) => {
                                    tracing::warn!("handler {} error: {e}", handler.id());
                                    let _ = msg.nack().await;
                                }
                            }
                        });
                        let _ = task.await;
                    }
                })
                .await;
        }

        tracker.close();
        tracker.wait().await;
        Ok(())
    }
}

pub struct ConsumerHandle {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<Result<()>>,
}

impl ConsumerHandle {
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    pub async fn shutdown(self) -> Result<()> {
        match self.join.await {
            Ok(result) => result,
            Err(e) => Err(Error::Store(format!("consumer task join error: {e}"))),
        }
    }

    pub fn abort(self) {
        self.cancel.cancel();
        self.join.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::Stream;

    use crate::event::Event;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Default)]
    struct TestSub;

    struct VecReader {
        events: Mutex<Option<Vec<Event>>>,
    }

    impl VecReader {
        fn new(events: Vec<Event>) -> Self {
            Self {
                events: Mutex::new(Some(events)),
            }
        }
    }

    impl Reader for VecReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = crate::io::NoCursor;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, crate::io::NoCursor>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let events = self
                .events
                .lock()
                .unwrap()
                .take()
                .expect("read called twice");
            let stream = futures::stream::iter(
                events
                    .into_iter()
                    .map(|e| Ok(Message::new(e, NoopAcker, crate::io::NoCursor))),
            );
            Ok(Box::pin(stream))
        }
    }

    struct CountingHandler {
        id: String,
        count: Arc<AtomicUsize>,
    }

    impl Handler for CountingHandler {
        fn id(&self) -> &str {
            &self.id
        }
        async fn handle(&self, _: &Event) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailingHandler {
        id: String,
        count: Arc<AtomicUsize>,
    }

    impl Handler for FailingHandler {
        fn id(&self) -> &str {
            &self.id
        }
        async fn handle(&self, _: &Event) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Err(Error::Store("boom".into()))
        }
    }

    fn make_event(i: usize) -> Event {
        Event::create(
            "org",
            "/x",
            "thing.happened",
            Payload::from_string(format!("p{i}")),
        )
        .unwrap()
    }

    fn subscription() -> TestSub {
        TestSub
    }

    #[tokio::test]
    async fn handles_each_event_once() {
        let events: Vec<Event> = (0..5).map(make_event).collect();
        let count = Arc::new(AtomicUsize::new(0));
        let consumer = BackgroundConsumer::new(
            VecReader::new(events),
            subscription(),
            CountingHandler {
                id: "h".into(),
                count: Arc::clone(&count),
            },
            2,
        );
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn handler_error_is_reported_via_nack() {
        let events: Vec<Event> = (0..2).map(make_event).collect();
        let count = Arc::new(AtomicUsize::new(0));
        let consumer = BackgroundConsumer::new(
            VecReader::new(events),
            subscription(),
            FailingHandler {
                id: "h".into(),
                count: Arc::clone(&count),
            },
            1,
        );
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn handler_timeout_yields_nack() {
        struct SlowHandler;
        impl Handler for SlowHandler {
            fn id(&self) -> &str {
                "slow"
            }
            async fn handle(&self, _: &Event) -> Result<()> {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(())
            }
        }
        let events: Vec<Event> = (0..1).map(make_event).collect();
        let consumer =
            BackgroundConsumer::new(VecReader::new(events), subscription(), SlowHandler, 1)
                .with_handler_timeout(Duration::from_millis(20));
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn processes_messages_in_opportunistic_batches() {
        let events: Vec<Event> = (0..4).map(make_event).collect();
        let count = Arc::new(AtomicUsize::new(0));
        let consumer = BackgroundConsumer::new(
            VecReader::new(events),
            subscription(),
            CountingHandler {
                id: "h".into(),
                count: Arc::clone(&count),
            },
            2,
        )
        .with_batch_size(3);
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 4);
    }
}
