use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eventuary_conformance::{
    AckFn, AckFuture, Backend, Capabilities, ConsumerEvent, ReaderRequest, run_all,
};
use eventuary_core::io::WriterExt;
use eventuary_core::{BoxWriter, EventSubscription, Result};
use eventuary_postgres::{PgDatabase, PgEventWriter, PgReader, PgReaderConfig};
use futures::StreamExt;
use sqlx::PgPool;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn start_postgres() -> (ContainerAsync<GenericImage>, PgPool) {
    let container = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "eventuary")
        .with_env_var("POSTGRES_PASSWORD", "eventuary")
        .with_env_var("POSTGRES_DB", "eventuary")
        .start()
        .await
        .expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://eventuary:eventuary@127.0.0.1:{port}/eventuary");
    let db = PgDatabase::connect(&url).await.expect("connect");
    let pool = db.pool();
    (container, pool)
}

struct PostgresBackend {
    _container: ContainerAsync<GenericImage>,
    pool: PgPool,
}

impl PostgresBackend {
    async fn new() -> Self {
        let (container, pool) = start_postgres().await;
        Self {
            _container: container,
            pool,
        }
    }

    fn reader_config(&self, request: &ReaderRequest) -> PgReaderConfig {
        PgReaderConfig {
            organization: request.organization.clone(),
            namespace: request.namespace.clone(),
            topics: request.topics.clone(),
            consumer_group_id: request.consumer_group_id.clone(),
            checkpoint_name: request.checkpoint_name.clone(),
            start_from: request.start_from,
            poll_interval: request.poll_interval,
            batch_size: 10,
        }
    }

    fn subscription(&self, request: &ReaderRequest) -> EventSubscription {
        let mut subscription = match request.organization.clone() {
            Some(organization) => EventSubscription::for_organization(organization),
            None => EventSubscription::new(),
        };
        subscription.checkpoint_name = Some(request.checkpoint_name.clone());
        subscription.consumer_group_id = request.consumer_group_id.clone();
        if !request.topics.is_empty() {
            subscription.topics = Some(request.topics.clone());
        }
        subscription.namespace_prefix = request.namespace.clone();
        subscription.start_from = request.start_from;
        subscription.partition = request.partition;
        subscription
    }
}

#[derive(Clone, Copy)]
enum AckOp {
    Ack,
    Nack,
}

fn make_ack(acker: Arc<eventuary_postgres::PgAckerVariant>, op: AckOp) -> AckFn {
    Box::new(move || -> AckFuture {
        Box::pin(async move {
            use eventuary_core::io::Acker;
            match op {
                AckOp::Ack => acker.ack().await,
                AckOp::Nack => acker.nack().await,
            }
        })
    })
}

impl Backend for PostgresBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities::full()
    }

    fn writer<'a>(&'a self) -> Pin<Box<dyn Future<Output = BoxWriter> + Send + 'a>> {
        Box::pin(async move { PgEventWriter::new(self.pool.clone()).into_boxed() })
    }

    fn read_one<'a>(
        &'a self,
        request: ReaderRequest,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Option<ConsumerEvent>> + Send + 'a>> {
        Box::pin(async move {
            let cfg = self.reader_config(&request);
            let subscription = self.subscription(&request);
            let reader = PgReader::new(self.pool.clone(), cfg);
            let mut stream = eventuary_core::io::Reader::read(&reader, subscription)
                .await
                .ok()?;
            let next = tokio::time::timeout(timeout, stream.next()).await.ok()??;
            let msg = next.ok()?;
            let (event, acker, _) = msg.into_parts();
            let shared = Arc::new(acker);
            Some(ConsumerEvent {
                event,
                ack: make_ack(Arc::clone(&shared), AckOp::Ack),
                nack: make_ack(shared, AckOp::Nack),
            })
        })
    }

    fn read_many<'a>(
        &'a self,
        request: ReaderRequest,
        count: usize,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Vec<ConsumerEvent>> + Send + 'a>> {
        Box::pin(async move {
            let cfg = self.reader_config(&request);
            let subscription = self.subscription(&request);
            let reader = PgReader::new(self.pool.clone(), cfg);
            let mut stream = match eventuary_core::io::Reader::read(&reader, subscription).await {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
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
                let (event, acker, _) = next.into_parts();
                let shared = Arc::new(acker);
                received.push(ConsumerEvent {
                    event,
                    ack: make_ack(Arc::clone(&shared), AckOp::Ack),
                    nack: make_ack(shared, AckOp::Nack),
                });
            }
            received
        })
    }

    fn read_result<'a>(
        &'a self,
        request: ReaderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let cfg = self.reader_config(&request);
            let subscription = self.subscription(&request);
            let reader = PgReader::new(self.pool.clone(), cfg);
            eventuary_core::io::Reader::read(&reader, subscription)
                .await
                .map(|_| ())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_backend_conformance() {
    let backend = PostgresBackend::new().await;
    run_all(&backend).await;
}
