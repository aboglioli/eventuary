//! Distributed subscriber work routing and reading.
//!
//! [`SubscriberWorkRouter`] is a [`Handler`] that, on each delivered event,
//! enqueues per-subscriber work items into a [`SubscriberWorkStore`] for every
//! route whose filter matches the event. The upstream message is acked once all
//! enqueues succeed, decoupling source progress from subscriber processing.
//!
//! [`SubscriberWorkReader`] polls a [`SubscriberWorkStore`] for a single
//! subscriber and produces a stream of [`Message<SubscriberWorkAcker, NoCursor>`]
//! for downstream handlers to consume independently.

use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::error::Result;
use crate::event::Event;
use crate::io::Handler;
use crate::io::filter::EventFilter;
use crate::io::handler::SubscriberId;
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, NoCursor, Reader};

pub trait SubscriberWorkStore: Clone + Send + Sync + 'static {
    type Id: Clone + Send + Sync + 'static;

    fn enqueue<'a>(
        &'a self,
        subscriber_id: &'a SubscriberId,
        event: &'a Event,
    ) -> impl Future<Output = Result<Self::Id>> + Send + 'a;

    fn claim_batch<'a>(
        &'a self,
        subscriber_id: &'a SubscriberId,
        max: usize,
        visibility: Duration,
    ) -> impl Future<Output = Result<Vec<SubscriberWorkItem<Self::Id>>>> + Send + 'a;

    fn ack<'a>(&'a self, id: &'a Self::Id) -> impl Future<Output = Result<()>> + Send + 'a;

    fn nack<'a>(&'a self, id: &'a Self::Id) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[derive(Debug, Clone)]
pub struct SubscriberWorkItem<Id> {
    pub id: Id,
    pub subscriber_id: SubscriberId,
    pub event: Event,
    pub attempts: u32,
}

pub struct SubscriberWorkRoute {
    pub subscriber_id: SubscriberId,
    pub filter: EventFilter,
}

pub struct SubscriberWorkRouter<S: SubscriberWorkStore> {
    store: S,
    routes: Vec<SubscriberWorkRoute>,
    handler_id: String,
}

impl<S: SubscriberWorkStore> SubscriberWorkRouter<S> {
    pub fn new(store: S, routes: Vec<SubscriberWorkRoute>, handler_id: impl Into<String>) -> Self {
        Self {
            store,
            routes,
            handler_id: handler_id.into(),
        }
    }
}

impl<S: SubscriberWorkStore> Handler for SubscriberWorkRouter<S> {
    fn id(&self) -> &str {
        &self.handler_id
    }

    async fn handle(&self, event: &Event) -> Result<()> {
        for route in &self.routes {
            if route.filter.matches(event) {
                self.store.enqueue(&route.subscriber_id, event).await?;
            }
        }
        Ok(())
    }
}

pub struct SubscriberWorkSubscription {
    pub subscriber_id: SubscriberId,
    pub batch_size: usize,
    pub poll_interval: Duration,
    pub visibility: Duration,
}

pub struct SubscriberWorkAcker<S: SubscriberWorkStore> {
    store: S,
    id: S::Id,
}

impl<S: SubscriberWorkStore> Acker for SubscriberWorkAcker<S> {
    async fn ack(&self) -> Result<()> {
        self.store.ack(&self.id).await
    }

    async fn nack(&self) -> Result<()> {
        self.store.nack(&self.id).await
    }
}

pub struct SubscriberWorkReader<S: SubscriberWorkStore> {
    store: S,
}

impl<S: SubscriberWorkStore> SubscriberWorkReader<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S: SubscriberWorkStore> Reader for SubscriberWorkReader<S> {
    type Subscription = SubscriberWorkSubscription;
    type Acker = SubscriberWorkAcker<S>;
    type Cursor = NoCursor;
    type Stream = SpawnedStream<SubscriberWorkAcker<S>, NoCursor>;

    async fn read(&self, sub: Self::Subscription) -> Result<Self::Stream> {
        let store = self.store.clone();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            loop {
                match store
                    .claim_batch(&sub.subscriber_id, sub.batch_size, sub.visibility)
                    .await
                {
                    Ok(items) if items.is_empty() => {
                        tokio::time::sleep(sub.poll_interval).await;
                        if tx.is_closed() {
                            return;
                        }
                    }
                    Ok(items) => {
                        for item in items {
                            let acker = SubscriberWorkAcker {
                                store: store.clone(),
                                id: item.id,
                            };
                            let msg = Message::new(item.event, acker, NoCursor);
                            if tx.send(Ok(msg)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}
