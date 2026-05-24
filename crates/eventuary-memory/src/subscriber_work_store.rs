use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use eventuary_core::Event;
use eventuary_core::Result;
use eventuary_core::io::handler::{SubscriberId, SubscriberWorkItem, SubscriberWorkStore};

#[derive(Debug)]
struct Entry {
    subscriber_id: SubscriberId,
    event: Event,
    claimed_until: Option<DateTime<Utc>>,
    attempts: u32,
}

#[derive(Debug, Default)]
struct State {
    next_id: u64,
    entries: BTreeMap<u64, Entry>,
}

#[derive(Debug, Clone, Default)]
pub struct MemorySubscriberWorkStore {
    state: Arc<Mutex<State>>,
}

impl MemorySubscriberWorkStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SubscriberWorkStore for MemorySubscriberWorkStore {
    type Id = u64;

    async fn enqueue(&self, subscriber_id: &SubscriberId, event: &Event) -> Result<Self::Id> {
        let mut state = self.state.lock().await;
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .expect("subscriber work store id space exhausted");
        state.entries.insert(
            id,
            Entry {
                subscriber_id: subscriber_id.clone(),
                event: event.clone(),
                claimed_until: None,
                attempts: 0,
            },
        );
        Ok(id)
    }

    async fn claim_batch(
        &self,
        subscriber_id: &SubscriberId,
        max: usize,
        visibility: Duration,
    ) -> Result<Vec<SubscriberWorkItem<Self::Id>>> {
        let mut state = self.state.lock().await;
        let now = Utc::now();
        let until = now + chrono::Duration::from_std(visibility).unwrap_or(chrono::Duration::MAX);
        let mut claimed = Vec::new();

        for (&id, entry) in state.entries.iter_mut() {
            if claimed.len() >= max {
                break;
            }
            if &entry.subscriber_id != subscriber_id {
                continue;
            }
            let available = entry.claimed_until.map(|t| t <= now).unwrap_or(true);
            if !available {
                continue;
            }
            entry.claimed_until = Some(until);
            entry.attempts += 1;
            claimed.push(SubscriberWorkItem {
                id,
                subscriber_id: entry.subscriber_id.clone(),
                event: entry.event.clone(),
                attempts: entry.attempts,
            });
        }

        Ok(claimed)
    }

    async fn ack(&self, id: &Self::Id) -> Result<()> {
        self.state.lock().await.entries.remove(id);
        Ok(())
    }

    async fn nack(&self, id: &Self::Id) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(entry) = state.entries.get_mut(id) {
            entry.claimed_until = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use eventuary_core::Payload;
    use eventuary_core::io::Reader;
    use eventuary_core::io::filter::EventFilter;
    use eventuary_core::io::handler::{
        Handler, SubscriberWorkReader, SubscriberWorkRoute, SubscriberWorkRouter,
        SubscriberWorkSubscription,
    };

    use super::*;

    fn ev(topic: &str) -> Event {
        Event::builder("acme", "/x", topic, Payload::from_string("p"))
            .unwrap()
            .build()
            .unwrap()
    }

    fn sub(s: &str) -> SubscriberId {
        SubscriberId::new(s).unwrap()
    }

    #[tokio::test]
    async fn enqueue_then_claim_returns_item_for_matching_subscriber() {
        let store = MemorySubscriberWorkStore::new();
        let sub_a = sub("sub-a");
        store.enqueue(&sub_a, &ev("thing.happened")).await.unwrap();

        let batch = store
            .claim_batch(&sub_a, 10, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].attempts, 1);
        assert_eq!(batch[0].subscriber_id, sub_a);
    }

    #[tokio::test]
    async fn claim_for_other_subscriber_returns_none() {
        let store = MemorySubscriberWorkStore::new();
        store
            .enqueue(&sub("sub-a"), &ev("thing.happened"))
            .await
            .unwrap();

        let batch = store
            .claim_batch(&sub("sub-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(batch.len(), 0);
    }

    #[tokio::test]
    async fn router_writes_per_subscriber_items() {
        let store = MemorySubscriberWorkStore::new();
        let router = SubscriberWorkRouter::new(
            store.clone(),
            vec![
                SubscriberWorkRoute {
                    subscriber_id: sub("sub-a"),
                    filter: EventFilter::default(),
                },
                SubscriberWorkRoute {
                    subscriber_id: sub("sub-b"),
                    filter: EventFilter::default(),
                },
            ],
            "router",
        );

        router.handle(&ev("order.created")).await.unwrap();

        let batch_a = store
            .claim_batch(&sub("sub-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        let batch_b = store
            .claim_batch(&sub("sub-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(batch_a.len(), 1);
        assert_eq!(batch_b.len(), 1);
    }

    #[tokio::test]
    async fn router_respects_filters() {
        let store = MemorySubscriberWorkStore::new();

        let filter_orders = EventFilter {
            topic: Some(eventuary_core::TopicPattern::exact(
                eventuary_core::Topic::new("orders.placed").unwrap(),
            )),
            ..Default::default()
        };
        let filter_payments = EventFilter {
            topic: Some(eventuary_core::TopicPattern::exact(
                eventuary_core::Topic::new("payments.charged").unwrap(),
            )),
            ..Default::default()
        };

        let router = SubscriberWorkRouter::new(
            store.clone(),
            vec![
                SubscriberWorkRoute {
                    subscriber_id: sub("sub-a"),
                    filter: filter_orders,
                },
                SubscriberWorkRoute {
                    subscriber_id: sub("sub-b"),
                    filter: filter_payments,
                },
            ],
            "router",
        );

        router.handle(&ev("orders.placed")).await.unwrap();

        let batch_a = store
            .claim_batch(&sub("sub-a"), 10, Duration::from_secs(60))
            .await
            .unwrap();
        let batch_b = store
            .claim_batch(&sub("sub-b"), 10, Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(batch_a.len(), 1);
        assert_eq!(batch_b.len(), 0);
    }

    #[tokio::test]
    async fn reader_delivers_pending_then_ack_removes() {
        let store = MemorySubscriberWorkStore::new();
        let sub_a = sub("sub-a");

        store.enqueue(&sub_a, &ev("e1")).await.unwrap();
        store.enqueue(&sub_a, &ev("e2")).await.unwrap();

        let reader = SubscriberWorkReader::new(store.clone());
        let mut stream = reader
            .read(SubscriberWorkSubscription {
                subscriber_id: sub_a.clone(),
                batch_size: 10,
                poll_interval: Duration::from_millis(10),
                visibility: Duration::from_secs(60),
            })
            .await
            .unwrap();

        let msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let msg2 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        msg1.ack().await.unwrap();
        msg2.ack().await.unwrap();

        drop(stream);

        let remaining = store
            .claim_batch(&sub_a, 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(remaining.len(), 0);
    }

    #[tokio::test]
    async fn reader_nack_makes_item_visible_again() {
        let store = MemorySubscriberWorkStore::new();
        let sub_a = sub("sub-a");

        store.enqueue(&sub_a, &ev("thing.happened")).await.unwrap();

        let reader = SubscriberWorkReader::new(store.clone());
        let mut stream = reader
            .read(SubscriberWorkSubscription {
                subscriber_id: sub_a.clone(),
                batch_size: 10,
                poll_interval: Duration::from_millis(10),
                visibility: Duration::from_secs(60),
            })
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        msg.nack().await.unwrap();
        drop(stream);

        let batch = store
            .claim_batch(&sub_a, 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].attempts, 2);
    }

    #[tokio::test]
    async fn expired_claims_reappear() {
        let store = MemorySubscriberWorkStore::new();
        let sub_a = sub("sub-a");

        store.enqueue(&sub_a, &ev("thing.happened")).await.unwrap();

        let first = store
            .claim_batch(&sub_a, 10, Duration::from_millis(5))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].attempts, 1);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let second = store
            .claim_batch(&sub_a, 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].attempts, 2);
    }
}
