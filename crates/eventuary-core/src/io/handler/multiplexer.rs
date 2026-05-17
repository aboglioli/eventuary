//! Multiplexer handler: fans out one delivered event to multiple
//! filter+handler subscribers under a single ack/nack lifecycle.
//!
//! Each route has a stable `SubscriberId` used as the storage key for
//! the optional `MultiplexerStore`. When configured, the store skips
//! subscribers that already completed for a given event on retry, so
//! redelivery from the backend does not re-run already-acked work.
//!
//! Without a store (the default `NoMultiplexerStore`), every matching
//! subscriber runs on every delivery — at-least-once at the
//! subscriber level matches the backend's at-least-once semantics.
//!
//! Subscribers run concurrently up to `concurrency` via a
//! `FuturesUnordered` drain loop. The handle aggregates every
//! subscriber failure into a single `Error::Handler`; no fail-fast
//! shortcut — slow successors still run so the handler reports the
//! full failure set.
//!
//! Failure modes affecting delivery semantics:
//! - Handler succeeds but `mark_completed` fails: at-least-once
//!   double-run on the next redelivery. Callers should accept idempotent
//!   handlers or pair this with the dedupe wrapper.
//! - Empty match set: governed by `NoMatchPolicy` (default `Ack`).

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::event::{Event, EventId};
use crate::io::{ArcFilter, ArcHandler, Filter, Handler, HandlerExt};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SubscriberId(Arc<str>);

impl SubscriberId {
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "invalid subscriber id length: {value:?}"
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
        {
            return Err(Error::Config(format!("invalid subscriber id: {value:?}")));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct MultiplexerKey {
    pub event_id: EventId,
    pub subscriber_id: SubscriberId,
}

impl MultiplexerKey {
    pub fn new(event_id: EventId, subscriber_id: SubscriberId) -> Self {
        Self {
            event_id,
            subscriber_id,
        }
    }
}

pub trait MultiplexerStore: Clone + Send + Sync + 'static {
    fn is_completed<'a>(
        &'a self,
        key: &'a MultiplexerKey,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;

    fn mark_completed<'a>(
        &'a self,
        key: &'a MultiplexerKey,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoMultiplexerStore;

impl MultiplexerStore for NoMultiplexerStore {
    async fn is_completed(&self, _: &MultiplexerKey) -> Result<bool> {
        Ok(false)
    }

    async fn mark_completed(&self, _: &MultiplexerKey) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct InMemoryStoreState {
    set: HashSet<MultiplexerKey>,
    order: VecDeque<MultiplexerKey>,
}

#[derive(Debug, Clone)]
pub struct InMemoryMultiplexerStore {
    state: Arc<Mutex<InMemoryStoreState>>,
    capacity: Option<NonZeroUsize>,
}

impl Default for InMemoryMultiplexerStore {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl InMemoryMultiplexerStore {
    pub fn unbounded() -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryStoreState {
                set: HashSet::new(),
                order: VecDeque::new(),
            })),
            capacity: None,
        }
    }

    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryStoreState {
                set: HashSet::new(),
                order: VecDeque::new(),
            })),
            capacity: Some(capacity),
        }
    }

    pub async fn len(&self) -> usize {
        self.state.lock().await.set.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.state.lock().await.set.is_empty()
    }

    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.set.clear();
        state.order.clear();
    }
}

impl MultiplexerStore for InMemoryMultiplexerStore {
    async fn is_completed(&self, key: &MultiplexerKey) -> Result<bool> {
        Ok(self.state.lock().await.set.contains(key))
    }

    /// Bounded variant evicts in **FIFO insertion order** when capacity
    /// is exceeded. Re-marking an already-completed key is a no-op and
    /// does not refresh recency.
    async fn mark_completed(&self, key: &MultiplexerKey) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.set.insert(key.clone()) {
            state.order.push_back(key.clone());
            if let Some(cap) = self.capacity {
                while state.set.len() > cap.get() {
                    if let Some(oldest) = state.order.pop_front() {
                        state.set.remove(&oldest);
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum NoMatchPolicy {
    #[default]
    Ack,
    Error,
}

struct MultiplexerRoute {
    subscriber_id: SubscriberId,
    filter: ArcFilter,
    handler: ArcHandler,
}

impl MultiplexerRoute {
    fn new<F, H>(subscriber_id: impl AsRef<str>, filter: F, handler: H) -> Result<Self>
    where
        F: Filter + 'static,
        H: Handler + 'static,
    {
        Ok(Self {
            subscriber_id: SubscriberId::new(subscriber_id)?,
            filter: Arc::new(filter),
            handler: handler.into_arced(),
        })
    }
}

#[derive(Debug)]
struct MultiplexerFailure {
    subscriber_id: SubscriberId,
    error: Error,
}

const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN;

pub struct MultiplexerBuilder<S = NoMultiplexerStore> {
    id: String,
    routes: Vec<MultiplexerRoute>,
    subscriber_ids: HashSet<SubscriberId>,
    store: S,
    concurrency: NonZeroUsize,
    no_match: NoMatchPolicy,
}

impl Default for MultiplexerBuilder<NoMultiplexerStore> {
    fn default() -> Self {
        Self {
            id: "multiplexer".to_owned(),
            routes: Vec::new(),
            subscriber_ids: HashSet::new(),
            store: NoMultiplexerStore,
            concurrency: DEFAULT_CONCURRENCY,
            no_match: NoMatchPolicy::Ack,
        }
    }
}

impl<S> MultiplexerBuilder<S> {
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    pub fn route<F, H>(
        mut self,
        subscriber_id: impl AsRef<str>,
        filter: F,
        handler: H,
    ) -> Result<Self>
    where
        F: Filter + 'static,
        H: Handler + 'static,
    {
        let route = MultiplexerRoute::new(subscriber_id, filter, handler)?;
        if !self.subscriber_ids.insert(route.subscriber_id.clone()) {
            return Err(Error::Config(format!(
                "multiplexer {}: duplicate subscriber id {}",
                self.id, route.subscriber_id
            )));
        }
        self.routes.push(route);
        Ok(self)
    }

    pub fn concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn on_no_match(mut self, policy: NoMatchPolicy) -> Self {
        self.no_match = policy;
        self
    }

    pub fn store<S2: MultiplexerStore>(self, store: S2) -> MultiplexerBuilder<S2> {
        MultiplexerBuilder {
            id: self.id,
            routes: self.routes,
            subscriber_ids: self.subscriber_ids,
            store,
            concurrency: self.concurrency,
            no_match: self.no_match,
        }
    }
}

impl<S: MultiplexerStore> MultiplexerBuilder<S> {
    pub fn build(self) -> Result<Multiplexer<S>> {
        if self.id.trim().is_empty() {
            return Err(Error::Config("multiplexer id must not be empty".to_owned()));
        }
        Ok(Multiplexer {
            id: self.id,
            routes: self.routes,
            store: self.store,
            concurrency: self.concurrency,
            no_match: self.no_match,
        })
    }
}

pub struct Multiplexer<S = NoMultiplexerStore> {
    id: String,
    routes: Vec<MultiplexerRoute>,
    store: S,
    concurrency: NonZeroUsize,
    no_match: NoMatchPolicy,
}

impl Multiplexer<NoMultiplexerStore> {
    pub fn builder() -> MultiplexerBuilder<NoMultiplexerStore> {
        MultiplexerBuilder::default()
    }
}

impl<S: MultiplexerStore> Multiplexer<S> {
    async fn run_subscriber(
        &self,
        subscriber_id: SubscriberId,
        handler: ArcHandler,
        event: &Event,
    ) -> std::result::Result<(), MultiplexerFailure> {
        let key = MultiplexerKey::new(event.id(), subscriber_id.clone());
        match self.store.is_completed(&key).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => {
                return Err(MultiplexerFailure {
                    subscriber_id,
                    error,
                });
            }
        }

        if let Err(error) = handler.handle(event).await {
            return Err(MultiplexerFailure {
                subscriber_id,
                error,
            });
        }

        if let Err(error) = self.store.mark_completed(&key).await {
            return Err(MultiplexerFailure {
                subscriber_id,
                error,
            });
        }

        Ok(())
    }
}

impl<S: MultiplexerStore> Handler for Multiplexer<S> {
    fn id(&self) -> &str {
        &self.id
    }

    async fn handle(&self, event: &Event) -> Result<()> {
        let matching: Vec<(SubscriberId, ArcHandler)> = self
            .routes
            .iter()
            .filter(|route| route.filter.matches(event))
            .map(|route| (route.subscriber_id.clone(), Arc::clone(&route.handler)))
            .collect();

        if matching.is_empty() {
            return match self.no_match {
                NoMatchPolicy::Ack => Ok(()),
                NoMatchPolicy::Error => Err(Error::Handler(format!(
                    "multiplexer {} found no matching subscriber for event {}",
                    self.id,
                    event.id()
                ))),
            };
        }

        let concurrency = self.concurrency.get();
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut iter = matching.into_iter();
        let mut failures: Vec<MultiplexerFailure> = Vec::new();

        for _ in 0..concurrency {
            if let Some((subscriber_id, handler)) = iter.next() {
                in_flight.push(self.run_subscriber(subscriber_id, handler, event));
            } else {
                break;
            }
        }

        while let Some(result) = in_flight.next().await {
            if let Err(failure) = result {
                failures.push(failure);
            }
            if let Some((subscriber_id, handler)) = iter.next() {
                in_flight.push(self.run_subscriber(subscriber_id, handler, event));
            }
        }

        if failures.is_empty() {
            return Ok(());
        }

        let summary = failures
            .iter()
            .map(|f| format!("{}: {}", f.subscriber_id, f.error))
            .collect::<Vec<_>>()
            .join(", ");
        Err(Error::Handler(format!(
            "multiplexer {} failed subscribers: {summary}",
            self.id
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::event::{Event, EventId};
    use crate::io::filter::EventFilter;
    use crate::io::{Filter, Handler};
    use crate::payload::Payload;

    #[test]
    fn subscriber_id_accepts_stable_names() {
        let id = SubscriberId::new("orders-projection.v1:blue").unwrap();
        assert_eq!(id.as_str(), "orders-projection.v1:blue");
    }

    #[test]
    fn subscriber_id_rejects_invalid_values() {
        assert!(SubscriberId::new("").is_err());
        assert!(SubscriberId::new("bad space").is_err());
        assert!(SubscriberId::new("bad/slash").is_err());
        assert!(SubscriberId::new("a".repeat(129)).is_err());
    }

    #[test]
    fn multiplexer_key_exposes_components() {
        let event_id = EventId::new();
        let subscriber_id = SubscriberId::new("audit").unwrap();
        let key = MultiplexerKey::new(event_id, subscriber_id.clone());
        assert_eq!(key.event_id, event_id);
        assert_eq!(key.subscriber_id, subscriber_id);
    }

    #[tokio::test]
    async fn no_multiplexer_store_reports_nothing_completed() {
        let store = NoMultiplexerStore;
        let key = MultiplexerKey::new(EventId::new(), SubscriberId::new("audit").unwrap());
        assert!(!store.is_completed(&key).await.unwrap());
        store.mark_completed(&key).await.unwrap();
        assert!(!store.is_completed(&key).await.unwrap());
    }

    #[tokio::test]
    async fn in_memory_store_records_completion() {
        let store = InMemoryMultiplexerStore::unbounded();
        let key = MultiplexerKey::new(EventId::new(), SubscriberId::new("audit").unwrap());

        assert!(!store.is_completed(&key).await.unwrap());
        store.mark_completed(&key).await.unwrap();
        assert!(store.is_completed(&key).await.unwrap());
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn in_memory_store_evicts_oldest_when_capacity_exceeded() {
        let store = InMemoryMultiplexerStore::with_capacity(NonZeroUsize::new(2).unwrap());
        let k1 = MultiplexerKey::new(EventId::new(), SubscriberId::new("a").unwrap());
        let k2 = MultiplexerKey::new(EventId::new(), SubscriberId::new("b").unwrap());
        let k3 = MultiplexerKey::new(EventId::new(), SubscriberId::new("c").unwrap());
        store.mark_completed(&k1).await.unwrap();
        store.mark_completed(&k2).await.unwrap();
        store.mark_completed(&k3).await.unwrap();

        assert_eq!(store.len().await, 2);
        assert!(!store.is_completed(&k1).await.unwrap());
        assert!(store.is_completed(&k2).await.unwrap());
        assert!(store.is_completed(&k3).await.unwrap());
    }

    #[tokio::test]
    async fn in_memory_store_clear_resets_state() {
        let store = InMemoryMultiplexerStore::unbounded();
        store
            .mark_completed(&MultiplexerKey::new(
                EventId::new(),
                SubscriberId::new("a").unwrap(),
            ))
            .await
            .unwrap();
        assert!(!store.is_empty().await);
        store.clear().await;
        assert!(store.is_empty().await);
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

    struct FailingHandler;

    impl Handler for FailingHandler {
        fn id(&self) -> &str {
            "failing"
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            Err(Error::Store("handler failed".to_owned()))
        }
    }

    struct AllowNothing;

    impl Filter for AllowNothing {
        fn matches(&self, _: &Event) -> bool {
            false
        }
    }

    fn event() -> Event {
        Event::builder(
            "acme",
            "/orders",
            "order.created",
            Payload::from_string("p"),
        )
        .unwrap()
        .key("order-1")
        .unwrap()
        .build()
        .unwrap()
    }

    #[tokio::test]
    async fn multiplexer_invokes_every_matching_handler() {
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));
        let multiplexer = Multiplexer::builder()
            .route(
                "first",
                EventFilter::default(),
                CountingHandler {
                    id: "first-handler".to_owned(),
                    count: Arc::clone(&first),
                },
            )
            .unwrap()
            .route(
                "second",
                EventFilter::default(),
                CountingHandler {
                    id: "second-handler".to_owned(),
                    count: Arc::clone(&second),
                },
            )
            .unwrap()
            .build()
            .unwrap();

        multiplexer.handle(&event()).await.unwrap();

        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(second.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multiplexer_skips_non_matching_routes() {
        let count = Arc::new(AtomicUsize::new(0));
        let multiplexer = Multiplexer::builder()
            .route(
                "skipped",
                AllowNothing,
                CountingHandler {
                    id: "skipped-handler".to_owned(),
                    count: Arc::clone(&count),
                },
            )
            .unwrap()
            .build()
            .unwrap();

        multiplexer.handle(&event()).await.unwrap();

        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn no_match_error_policy_returns_handler_error() {
        let multiplexer = Multiplexer::builder()
            .route("skipped", AllowNothing, FailingHandler)
            .unwrap()
            .on_no_match(NoMatchPolicy::Error)
            .build()
            .unwrap();

        let err = multiplexer.handle(&event()).await.unwrap_err();
        assert!(matches!(err, Error::Handler(_)));
    }

    #[tokio::test]
    async fn matching_handler_failure_returns_handler_error() {
        let multiplexer = Multiplexer::builder()
            .route("failing", EventFilter::default(), FailingHandler)
            .unwrap()
            .build()
            .unwrap();

        let err = multiplexer.handle(&event()).await.unwrap_err();
        assert!(matches!(err, Error::Handler(_)));
    }

    #[tokio::test]
    async fn duplicate_subscriber_id_is_rejected_at_route() {
        let result = Multiplexer::builder()
            .route("same", EventFilter::default(), FailingHandler)
            .unwrap()
            .route("same", EventFilter::default(), FailingHandler);

        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn nested_multiplexer_composes() {
        let inner_count = Arc::new(AtomicUsize::new(0));
        let inner = Multiplexer::builder()
            .id("inner")
            .route(
                "inner-sub",
                EventFilter::default(),
                CountingHandler {
                    id: "inner-handler".to_owned(),
                    count: Arc::clone(&inner_count),
                },
            )
            .unwrap()
            .build()
            .unwrap();

        let outer = Multiplexer::builder()
            .id("outer")
            .route("outer-sub", EventFilter::default(), inner)
            .unwrap()
            .build()
            .unwrap();

        outer.handle(&event()).await.unwrap();
        assert_eq!(inner_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn store_skips_completed_subscribers_on_redelivery() {
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));
        let multiplexer = Multiplexer::builder()
            .route(
                "first",
                EventFilter::default(),
                CountingHandler {
                    id: "first-handler".to_owned(),
                    count: Arc::clone(&first),
                },
            )
            .unwrap()
            .route(
                "second",
                EventFilter::default(),
                CountingHandler {
                    id: "second-handler".to_owned(),
                    count: Arc::clone(&second),
                },
            )
            .unwrap()
            .store(InMemoryMultiplexerStore::unbounded())
            .build()
            .unwrap();
        let event = event();

        multiplexer.handle(&event).await.unwrap();
        multiplexer.handle(&event).await.unwrap();

        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(second.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn redelivery_runs_only_subscriber_that_did_not_complete() {
        let completed = Arc::new(AtomicUsize::new(0));
        let multiplexer = Multiplexer::builder()
            .route(
                "completed",
                EventFilter::default(),
                CountingHandler {
                    id: "completed-handler".to_owned(),
                    count: Arc::clone(&completed),
                },
            )
            .unwrap()
            .route("failing", EventFilter::default(), FailingHandler)
            .unwrap()
            .store(InMemoryMultiplexerStore::unbounded())
            .build()
            .unwrap();
        let event = event();

        assert!(multiplexer.handle(&event).await.is_err());
        assert!(multiplexer.handle(&event).await.is_err());

        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[derive(Clone)]
    struct MarkFailsStore;

    impl MultiplexerStore for MarkFailsStore {
        async fn is_completed(&self, _: &MultiplexerKey) -> Result<bool> {
            Ok(false)
        }

        async fn mark_completed(&self, _: &MultiplexerKey) -> Result<()> {
            Err(Error::Store("mark failed".to_owned()))
        }
    }

    #[tokio::test]
    async fn mark_completed_failure_surfaces_as_handler_error() {
        let count = Arc::new(AtomicUsize::new(0));
        let multiplexer = Multiplexer::builder()
            .route(
                "sub",
                EventFilter::default(),
                CountingHandler {
                    id: "h".to_owned(),
                    count: Arc::clone(&count),
                },
            )
            .unwrap()
            .store(MarkFailsStore)
            .build()
            .unwrap();

        let err = multiplexer.handle(&event()).await.unwrap_err();
        assert!(matches!(err, Error::Handler(_)));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrency_greater_than_one_runs_handlers() {
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));
        let multiplexer = Multiplexer::builder()
            .route(
                "first",
                EventFilter::default(),
                CountingHandler {
                    id: "first-handler".to_owned(),
                    count: Arc::clone(&first),
                },
            )
            .unwrap()
            .route(
                "second",
                EventFilter::default(),
                CountingHandler {
                    id: "second-handler".to_owned(),
                    count: Arc::clone(&second),
                },
            )
            .unwrap()
            .concurrency(NonZeroUsize::new(4).unwrap())
            .build()
            .unwrap();

        multiplexer.handle(&event()).await.unwrap();

        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(second.load(Ordering::SeqCst), 1);
    }
}
