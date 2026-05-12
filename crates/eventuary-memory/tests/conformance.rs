use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eventuary_conformance::{
    AckFn, AckFuture, Backend, Capabilities, ConsumerEvent, ReaderRequest, run_all,
};
use eventuary_core::io::WriterExt;
use eventuary_core::{BoxWriter, EventSubscription, StartFrom};
use eventuary_memory::{InmemReader, InmemWriter};
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};

const CHANNEL_CAPACITY: usize = 256;

struct MemoryBackend {
    state: Mutex<Option<ChannelPair>>,
}

struct ChannelPair {
    reader: Arc<Mutex<Option<InmemReader>>>,
}

impl MemoryBackend {
    fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }

    async fn reset(&self) -> mpsc::Sender<eventuary_core::Event> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let reader = InmemReader::new(rx);
        let mut guard = self.state.lock().await;
        *guard = Some(ChannelPair {
            reader: Arc::new(Mutex::new(Some(reader))),
        });
        tx
    }

    async fn current_reader(&self) -> Arc<Mutex<Option<InmemReader>>> {
        let guard = self.state.lock().await;
        Arc::clone(
            &guard
                .as_ref()
                .expect("memory backend must be initialized via writer() before reading")
                .reader,
        )
    }
}

fn noop_ack() -> AckFn {
    Box::new(|| -> AckFuture { Box::pin(async { Ok(()) }) })
}

fn build_subscription(request: &ReaderRequest) -> EventSubscription {
    let mut subscription = match request.organization.clone() {
        Some(organization) => EventSubscription::for_organization(organization),
        None => EventSubscription::new(),
    };
    if !request.topics.is_empty() {
        subscription.topics = Some(request.topics.clone());
    }
    subscription.namespace_prefix = request.namespace.clone();
    subscription
}

impl Backend for MemoryBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            supports_replay: false,
            supports_timestamp_start: false,
            supports_nack_redelivery: false,
            preserves_total_order: true,
            supports_consumer_groups: false,
            supports_independent_checkpoints: false,
            supports_runtime_partitioning: false,
        }
    }

    fn writer<'a>(&'a self) -> Pin<Box<dyn Future<Output = BoxWriter> + Send + 'a>> {
        Box::pin(async move {
            let tx = self.reset().await;
            InmemWriter::new(tx).into_boxed()
        })
    }

    fn read_one<'a>(
        &'a self,
        request: ReaderRequest,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Option<ConsumerEvent>> + Send + 'a>> {
        Box::pin(async move {
            let reader_handle = self.current_reader().await;
            let subscription = build_subscription(&request);
            let mut guard = reader_handle.lock().await;
            let reader = guard.as_mut()?;
            let mut stream = eventuary_core::io::Reader::read(reader, subscription.clone())
                .await
                .ok()?;

            if matches!(request.start_from, StartFrom::Latest) {
                drain_pending(&mut stream).await;
            }

            let deadline = Instant::now() + timeout;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                let next = match tokio::time::timeout(remaining, stream.next()).await {
                    Ok(Some(Ok(msg))) => msg,
                    _ => return None,
                };
                let event = next.into_event();
                if subscription.matches(&event) {
                    return Some(ConsumerEvent {
                        event,
                        ack: noop_ack(),
                        nack: noop_ack(),
                    });
                }
            }
        })
    }

    fn read_many<'a>(
        &'a self,
        request: ReaderRequest,
        count: usize,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Vec<ConsumerEvent>> + Send + 'a>> {
        Box::pin(async move {
            let reader_handle = self.current_reader().await;
            let subscription = build_subscription(&request);
            let mut guard = reader_handle.lock().await;
            let Some(reader) = guard.as_mut() else {
                return Vec::new();
            };
            let mut stream =
                match eventuary_core::io::Reader::read(reader, subscription.clone()).await {
                    Ok(s) => s,
                    Err(_) => return Vec::new(),
                };

            if matches!(request.start_from, StartFrom::Latest) {
                drain_pending(&mut stream).await;
            }

            let mut received = Vec::with_capacity(count);
            let deadline = Instant::now() + timeout;
            while received.len() < count {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let next = match tokio::time::timeout(remaining, stream.next()).await {
                    Ok(Some(Ok(msg))) => msg,
                    _ => break,
                };
                let event = next.into_event();
                if subscription.matches(&event) {
                    received.push(ConsumerEvent {
                        event,
                        ack: noop_ack(),
                        nack: noop_ack(),
                    });
                }
            }
            received
        })
    }
}

async fn drain_pending<S>(stream: &mut S)
where
    S: futures::Stream<
            Item = eventuary_core::Result<
                eventuary_core::io::Message<eventuary_core::io::acker::NoopAcker>,
            >,
        > + Unpin,
{
    while let Ok(Some(Ok(_))) = tokio::time::timeout(Duration::from_millis(10), stream.next()).await
    {
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_backend_conformance() {
    let backend = MemoryBackend::new();
    run_all(&backend).await;
}
