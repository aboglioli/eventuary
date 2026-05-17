use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::io::Reader;
use crate::io::stream::SpawnedStream;

#[derive(Debug, Clone)]
pub struct RecoverConfig {
    max_retries: usize,
    backoff: Duration,
    backoff_multiplier: f64,
}

impl RecoverConfig {
    pub fn new(max_retries: usize, backoff: Duration, backoff_multiplier: f64) -> Result<Self> {
        if !backoff_multiplier.is_finite() || backoff_multiplier < 1.0 {
            return Err(Error::Config(format!(
                "recover backoff_multiplier must be finite and >= 1.0, got {backoff_multiplier}"
            )));
        }
        Ok(Self {
            max_retries,
            backoff,
            backoff_multiplier,
        })
    }

    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    pub fn backoff(&self) -> Duration {
        self.backoff
    }

    pub fn backoff_multiplier(&self) -> f64 {
        self.backoff_multiplier
    }
}

impl Default for RecoverConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: Duration::from_millis(100),
            backoff_multiplier: 2.0,
        }
    }
}

pub struct RecoverReader<R> {
    inner: R,
    config: RecoverConfig,
}

impl<R> RecoverReader<R> {
    pub fn new(inner: R, config: RecoverConfig) -> Self {
        Self { inner, config }
    }
}

impl<R> Reader for RecoverReader<R>
where
    R: Reader + Clone + Send + Sync + 'static,
    R::Subscription: Clone + Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<R::Acker, R::Cursor>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner_reader = self.inner.clone();
        let config = self.config.clone();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let mut retries = 0usize;
            let mut backoff = config.backoff;
            let mut stream = match inner_reader.read(subscription.clone()).await {
                Ok(s) => Box::pin(s),
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            loop {
                match stream.next().await {
                    Some(Ok(msg)) => {
                        retries = 0;
                        backoff = config.backoff;
                        if tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                    }
                    Some(Err(e)) => {
                        if retries >= config.max_retries {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                        retries += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = Duration::from_secs_f64(
                            backoff.as_secs_f64() * config.backoff_multiplier,
                        );
                        if let Ok(s) = inner_reader.read(subscription.clone()).await {
                            stream = Box::pin(s);
                        }
                    }
                    None => return,
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::{Stream, StreamExt, stream};

    use super::*;
    use crate::event::Event;
    use crate::io::Message;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    #[derive(Clone)]
    struct AlternatingReader {
        call_count: std::sync::Arc<AtomicUsize>,
    }

    impl Reader for AlternatingReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let call = self.call_count.fetch_add(1, Ordering::SeqCst);
            let items: Vec<Result<Message<NoopAcker, TestCursor>>> = if call < 2 {
                vec![Err(crate::Error::Store("transient".into()))]
            } else {
                vec![Ok(Message::new(
                    Event::create("org", "/x", "test", Payload::from_string("p")).unwrap(),
                    NoopAcker,
                    TestCursor(0),
                ))]
            };
            Ok(Box::pin(stream::iter(items)))
        }
    }

    #[tokio::test]
    async fn retries_and_then_produces_message() {
        let call_count = std::sync::Arc::new(AtomicUsize::new(0));
        let reader = AlternatingReader {
            call_count: std::sync::Arc::clone(&call_count),
        };
        let config = RecoverConfig::new(3, Duration::from_millis(1), 1.0).unwrap();
        let recover = RecoverReader::new(reader, config);
        let mut stream = recover.read(()).await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(*msg.cursor(), TestCursor(0));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn stops_after_max_retries() {
        let call_count = std::sync::Arc::new(AtomicUsize::new(0));
        let reader = AlternatingReader {
            call_count: std::sync::Arc::clone(&call_count),
        };
        let config = RecoverConfig::new(1, Duration::from_millis(1), 1.0).unwrap();
        let recover = RecoverReader::new(reader, config);
        let mut stream = recover.read(()).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();

        assert!(result.unwrap().is_err());
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn works_with_arc_wrapped_non_clone_reader() {
        type Items = Vec<Result<Message<NoopAcker, TestCursor>>>;

        struct NonCloneReader {
            events: std::sync::Mutex<Option<Items>>,
        }

        impl Reader for NonCloneReader {
            type Subscription = ();
            type Acker = NoopAcker;
            type Cursor = TestCursor;
            type Stream =
                Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

            async fn read(&self, _: ()) -> Result<Self::Stream> {
                let events = self.events.lock().unwrap().take().unwrap_or_default();
                Ok(Box::pin(stream::iter(events)))
            }
        }

        let event = Message::new(
            Event::create("org", "/x", "test", Payload::from_string("p")).unwrap(),
            NoopAcker,
            TestCursor(0),
        );
        let items: Items = vec![Ok(event)];
        let reader = std::sync::Arc::new(NonCloneReader {
            events: std::sync::Mutex::new(Some(items)),
        });
        let config = RecoverConfig::new(3, Duration::from_millis(1), 1.0).unwrap();
        let recover = RecoverReader::new(reader, config);
        let mut stream = recover.read(()).await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(*msg.cursor(), TestCursor(0));
    }

    #[test]
    fn recover_config_rejects_subunit_multiplier() {
        assert!(RecoverConfig::new(3, Duration::from_millis(1), 0.5).is_err());
    }

    #[test]
    fn recover_config_rejects_non_finite_multiplier() {
        assert!(RecoverConfig::new(3, Duration::from_millis(1), f64::NAN).is_err());
        assert!(RecoverConfig::new(3, Duration::from_millis(1), f64::INFINITY).is_err());
    }
}
