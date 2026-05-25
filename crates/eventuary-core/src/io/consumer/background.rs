use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::error::{Error, Result};
use crate::io::acker::NackContext;
use crate::io::{Handler, Reader};
use crate::payload::Payload;

pub struct BackgroundConsumer<R, H, P = Payload>
where
    R: Reader<P>,
    H: Handler<P>,
    P: Send + Sync,
{
    reader: R,
    subscription: R::Subscription,
    handler: Arc<H>,
    concurrency: usize,
    batch_size: usize,
    handler_timeout: Option<Duration>,
    _payload: PhantomData<P>,
}

impl<R, H, P> BackgroundConsumer<R, H, P>
where
    R: Reader<P> + Send + 'static,
    R::Stream: 'static,
    R::Cursor: Send + Sync + 'static,
    H: Handler<P> + 'static,
    P: Send + Sync + 'static,
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
            _payload: PhantomData,
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
                                    let context = match &e {
                                        Error::Timeout(message) => NackContext::handler_timeout(
                                            handler.id(),
                                            message.clone(),
                                        ),
                                        _ => NackContext::handler_error(handler.id(), e.clone()),
                                    };
                                    match context {
                                        Ok(context) => {
                                            let _ = msg.nack_with(context).await;
                                        }
                                        Err(error) => {
                                            tracing::warn!("failed to build nack context: {error}");
                                            let _ = msg.nack().await;
                                        }
                                    }
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

    use crate::context::ContextValue;
    use crate::event::Event;
    use crate::io::Message;
    use crate::io::NoCursor;
    use crate::io::acker::{NackReason, NoopAcker};
    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct ContextAcker {
        contexts: Arc<Mutex<Vec<NackContext>>>,
    }

    impl ContextAcker {
        fn contexts(&self) -> Vec<NackContext> {
            self.contexts.lock().unwrap().clone()
        }
    }

    impl crate::io::Acker for ContextAcker {
        async fn ack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
        async fn nack_with(&self, context: NackContext) -> Result<()> {
            self.contexts.lock().unwrap().push(context);
            Ok(())
        }
    }

    type ContextMessage = Message<ContextAcker, NoCursor>;

    struct ContextReader {
        items: Mutex<Option<Vec<Result<ContextMessage>>>>,
    }

    impl Reader for ContextReader {
        type Subscription = TestSub;
        type Acker = ContextAcker;
        type Cursor = NoCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<ContextMessage>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let items = self.items.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(items)))
        }
    }

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
        type Cursor = NoCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, NoCursor>>> + Send>>;

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
                    .map(|e| Ok(Message::new(e, NoopAcker, NoCursor))),
            );
            Ok(Box::pin(stream))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    struct TypedVecReader {
        events: Mutex<Option<Vec<Event<UserUpdated>>>>,
    }

    impl TypedVecReader {
        fn new(events: Vec<Event<UserUpdated>>) -> Self {
            Self {
                events: Mutex::new(Some(events)),
            }
        }
    }

    impl Reader<UserUpdated> for TypedVecReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = NoCursor;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, NoCursor, UserUpdated>>> + Send>>;

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
                    .map(|e| Ok(Message::new(e, NoopAcker, NoCursor))),
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

    struct TypedCountingHandler {
        id: String,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl Handler<UserUpdated> for TypedCountingHandler {
        fn id(&self) -> &str {
            &self.id
        }

        async fn handle(&self, event: &Event<UserUpdated>) -> Result<()> {
            self.seen
                .lock()
                .unwrap()
                .push(event.payload().user_id.clone());
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
            "thing-1",
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
    async fn handles_typed_events() {
        let events = vec![
            Event::create(
                "org",
                "/users",
                "user.updated",
                "thing-1",
                UserUpdated {
                    user_id: "u-1".to_owned(),
                },
            )
            .unwrap(),
            Event::create(
                "org",
                "/users",
                "user.updated",
                "thing-1",
                UserUpdated {
                    user_id: "u-2".to_owned(),
                },
            )
            .unwrap(),
        ];
        let seen = Arc::new(Mutex::new(Vec::new()));
        let consumer = BackgroundConsumer::new(
            TypedVecReader::new(events),
            subscription(),
            TypedCountingHandler {
                id: "typed".into(),
                seen: Arc::clone(&seen),
            },
            2,
        );
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();

        let mut seen = seen.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, vec!["u-1".to_owned(), "u-2".to_owned()]);
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
    async fn handler_error_emits_handler_error_nack_context() {
        let acker = ContextAcker::default();
        let reader = ContextReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                make_event(0),
                acker.clone(),
                NoCursor,
            ))])),
        };
        let count = Arc::new(AtomicUsize::new(0));
        let consumer = BackgroundConsumer::new(
            reader,
            subscription(),
            FailingHandler {
                id: "billing".into(),
                count: Arc::clone(&count),
            },
            1,
        );
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();

        let contexts = acker.contexts();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].reason(), NackReason::HandlerError);
        assert_eq!(
            contexts[0].context().get("handler_id"),
            Some(&ContextValue::String("billing".to_owned()))
        );
    }

    #[tokio::test]
    async fn handler_timeout_emits_handler_timeout_nack_context() {
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

        let acker = ContextAcker::default();
        let reader = ContextReader {
            items: Mutex::new(Some(vec![Ok(Message::new(
                make_event(0),
                acker.clone(),
                NoCursor,
            ))])),
        };
        let consumer = BackgroundConsumer::new(reader, subscription(), SlowHandler, 1)
            .with_handler_timeout(Duration::from_millis(20));
        let handle = consumer.spawn();
        handle.shutdown().await.unwrap();

        let contexts = acker.contexts();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].reason(), NackReason::HandlerTimeout);
        assert_eq!(
            contexts[0].context().get("handler_id"),
            Some(&ContextValue::String("slow".to_owned()))
        );
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
