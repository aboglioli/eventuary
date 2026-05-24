use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Writer;
use crate::payload::Payload;

#[derive(Debug, Clone)]
pub struct BatchWriterConfig {
    max_events: NonZeroUsize,
    max_wait: Duration,
    channel_capacity: NonZeroUsize,
}

impl BatchWriterConfig {
    pub fn new(
        max_events: NonZeroUsize,
        max_wait: Duration,
        channel_capacity: NonZeroUsize,
    ) -> Result<Self> {
        if max_wait.is_zero() {
            return Err(Error::Config(
                "batch writer max_wait must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            max_events,
            max_wait,
            channel_capacity,
        })
    }

    pub fn max_events(&self) -> NonZeroUsize {
        self.max_events
    }

    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }

    pub fn channel_capacity(&self) -> NonZeroUsize {
        self.channel_capacity
    }
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self {
            max_events: NonZeroUsize::new(100).unwrap(),
            max_wait: Duration::from_millis(10),
            channel_capacity: NonZeroUsize::new(1024).unwrap(),
        }
    }
}

pub struct BatchWriter<P = Payload> {
    tx: mpsc::Sender<BatchWriteRequest<P>>,
    config: BatchWriterConfig,
}

impl<P> Clone for BatchWriter<P> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            config: self.config.clone(),
        }
    }
}

struct BatchWriteRequest<P> {
    events: Vec<Event<P>>,
    result: oneshot::Sender<Result<()>>,
}

impl<P> BatchWriter<P>
where
    P: Send + Sync + 'static,
{
    pub fn spawn<W>(inner: W, config: BatchWriterConfig) -> Self
    where
        W: Writer<P> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(config.channel_capacity.get());
        tokio::spawn(run_flusher(inner, rx, config.clone()));
        Self { tx, config }
    }

    pub fn config(&self) -> &BatchWriterConfig {
        &self.config
    }

    async fn submit(&self, events: Vec<Event<P>>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(BatchWriteRequest { events, result: tx })
            .await
            .map_err(|_| Error::Store("batch writer flusher is closed".to_owned()))?;
        rx.await
            .map_err(|_| Error::Store("batch writer flusher dropped response".to_owned()))?
    }
}

impl<P> Writer<P> for BatchWriter<P>
where
    P: Clone + Send + Sync + 'static,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        self.submit(vec![event.clone()]).await
    }

    async fn write_all(&self, events: &[Event<P>]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        for chunk in events.chunks(self.config.max_events.get()) {
            self.submit(chunk.to_vec()).await?;
        }
        Ok(())
    }
}

async fn run_flusher<W, P>(
    inner: W,
    mut rx: mpsc::Receiver<BatchWriteRequest<P>>,
    config: BatchWriterConfig,
) where
    W: Writer<P>,
    P: Send + Sync,
{
    while let Some(first) = rx.recv().await {
        let mut requests = vec![first];
        let mut event_count = requests[0].events.len();
        let delay = tokio::time::sleep(config.max_wait);
        tokio::pin!(delay);

        loop {
            if event_count >= config.max_events.get() {
                break;
            }

            tokio::select! {
                maybe_request = rx.recv() => {
                    match maybe_request {
                        Some(request) => {
                            event_count += request.events.len();
                            requests.push(request);
                        }
                        None => break,
                    }
                }
                _ = &mut delay => break,
            }
        }

        flush_requests(&inner, requests).await;
    }
}

async fn flush_requests<W, P>(inner: &W, mut requests: Vec<BatchWriteRequest<P>>)
where
    W: Writer<P>,
    P: Send + Sync,
{
    let events = requests
        .iter_mut()
        .flat_map(|request| std::mem::take(&mut request.events))
        .collect::<Vec<_>>();
    let result = inner.write_all(&events).await;

    for request in requests {
        let _ = request.result.send(result.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct CapturingWriter {
        batches: Arc<Mutex<Vec<Vec<Event>>>>,
    }

    impl CapturingWriter {
        fn batches(&self) -> Vec<Vec<Event>> {
            self.batches.lock().unwrap().clone()
        }
    }

    impl Writer for CapturingWriter {
        async fn write(&self, event: &Event) -> Result<()> {
            self.write_all(std::slice::from_ref(event)).await
        }

        async fn write_all(&self, events: &[Event]) -> Result<()> {
            self.batches.lock().unwrap().push(events.to_vec());
            Ok(())
        }
    }

    struct FailingWriter;

    impl Writer for FailingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            Err(Error::Store("batch failed".to_owned()))
        }

        async fn write_all(&self, _: &[Event]) -> Result<()> {
            Err(Error::Store("batch failed".to_owned()))
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    fn config(max_events: usize, max_wait: Duration) -> BatchWriterConfig {
        BatchWriterConfig::new(
            NonZeroUsize::new(max_events).unwrap(),
            max_wait,
            NonZeroUsize::new(16).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn batch_writer_config_rejects_zero_wait() {
        let err = BatchWriterConfig::new(
            NonZeroUsize::new(1).unwrap(),
            Duration::ZERO,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("max_wait"));
    }

    #[test]
    fn batch_writer_config_default_is_valid() {
        let config = BatchWriterConfig::default();
        assert_eq!(config.max_events().get(), 100);
        assert_eq!(config.max_wait(), Duration::from_millis(10));
    }

    #[tokio::test]
    async fn batch_writer_flushes_when_max_events_reached() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = BatchWriter::spawn(inner, config(2, Duration::from_secs(1)));

        let one = ev("one");
        let two = ev("two");
        let first = writer.write(&one);
        let second = writer.write(&two);
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();

        let batches = captured.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
    }

    #[tokio::test]
    async fn batch_writer_flushes_when_wait_expires() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = BatchWriter::spawn(inner, config(10, Duration::from_millis(20)));

        writer.write(&ev("one")).await.unwrap();

        let batches = captured.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
    }

    #[tokio::test]
    async fn batch_writer_propagates_flush_error_to_all_waiters() {
        let writer = BatchWriter::spawn(FailingWriter, config(2, Duration::from_secs(1)));

        let one = ev("one");
        let two = ev("two");
        let first = writer.write(&one);
        let second = writer.write(&two);
        let (first, second) = tokio::join!(first, second);

        assert!(first.unwrap_err().to_string().contains("batch failed"));
        assert!(second.unwrap_err().to_string().contains("batch failed"));
    }

    #[tokio::test]
    async fn batch_writer_write_all_splits_large_input() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = BatchWriter::spawn(inner, config(2, Duration::from_millis(1)));

        writer
            .write_all(&[ev("one"), ev("two"), ev("three")])
            .await
            .unwrap();

        let total = captured
            .batches()
            .into_iter()
            .map(|batch| batch.len())
            .sum::<usize>();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn batch_writer_write_all_empty_returns_ok() {
        let inner = CapturingWriter::default();
        let captured = inner.clone();
        let writer = BatchWriter::spawn(inner, config(2, Duration::from_millis(20)));

        writer.write_all(&[]).await.unwrap();

        assert!(captured.batches().is_empty());
    }
}
