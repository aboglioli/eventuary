use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eventuary_conformance::{
    AckFn, AckFuture, Backend, Capabilities, ConsumerEvent, ReaderRequest, run_all,
};
use eventuary_core::BoxWriter;
use eventuary_core::io::WriterExt;
use eventuary_sqlite::{SqliteDatabase, SqliteEventWriter, SqliteReader, SqliteReaderConfig};
use futures::StreamExt;
use tempfile::TempDir;

struct SqliteBackend {
    _dir: TempDir,
    db: Arc<SqliteDatabase>,
}

impl SqliteBackend {
    fn new() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("events.db");
        let db = SqliteDatabase::open(&path).expect("open sqlite");
        Self {
            _dir: dir,
            db: Arc::new(db),
        }
    }

    fn reader_config(&self, request: &ReaderRequest) -> SqliteReaderConfig {
        SqliteReaderConfig {
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
}

fn make_ack(acker: Arc<eventuary_sqlite::SqliteAckerVariant>, op: AckOp) -> AckFn {
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

#[derive(Clone, Copy)]
enum AckOp {
    Ack,
    Nack,
}

impl Backend for SqliteBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities::full()
    }

    fn writer<'a>(&'a self) -> Pin<Box<dyn Future<Output = BoxWriter> + Send + 'a>> {
        Box::pin(async move { SqliteEventWriter::new(self.db.conn()).into_boxed() })
    }

    fn read_one<'a>(
        &'a self,
        request: ReaderRequest,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Option<ConsumerEvent>> + Send + 'a>> {
        Box::pin(async move {
            let cfg = self.reader_config(&request);
            let reader = SqliteReader::new(self.db.conn(), cfg);
            let mut stream = reader.read().await.ok()?;
            let next = tokio::time::timeout(timeout, stream.next()).await.ok()??;
            let msg = next.ok()?;
            let (event, acker) = msg.into_parts();
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
            let reader = SqliteReader::new(self.db.conn(), cfg);
            let mut stream = match reader.read().await {
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
                let (event, acker) = next.into_parts();
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_backend_conformance() {
    let backend = SqliteBackend::new();
    run_all(&backend).await;
}
