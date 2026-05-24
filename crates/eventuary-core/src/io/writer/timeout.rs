use std::time::Duration;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Writer;

pub struct TimeoutWriter<W> {
    inner: W,
    timeout: Duration,
}

impl<W> TimeoutWriter<W> {
    pub fn new(inner: W, timeout: Duration) -> Self {
        Self { inner, timeout }
    }
}

impl<W, P> Writer<P> for TimeoutWriter<W>
where
    W: Writer<P>,
    P: Send + Sync,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        tokio::time::timeout(self.timeout, self.inner.write(event))
            .await
            .map_err(|_| Error::Timeout(format!("writer timed out after {:?}", self.timeout)))?
    }

    async fn write_all(&self, events: &[Event<P>]) -> Result<()> {
        tokio::time::timeout(self.timeout, self.inner.write_all(events))
            .await
            .map_err(|_| {
                Error::Timeout(format!("writer batch timed out after {:?}", self.timeout))
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::payload::Payload;

    struct SlowWriter;

    impl Writer for SlowWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
    }

    struct CountingWriter {
        count: Arc<AtomicUsize>,
    }

    impl Writer for CountingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn write_all(&self, events: &[Event]) -> Result<()> {
            self.count.fetch_add(events.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn timeout_writer_forwards_successful_write() {
        let count = Arc::new(AtomicUsize::new(0));
        let writer = TimeoutWriter::new(
            CountingWriter {
                count: Arc::clone(&count),
            },
            Duration::from_secs(1),
        );

        writer.write(&ev()).await.unwrap();

        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn timeout_writer_times_out_slow_write() {
        let writer = TimeoutWriter::new(SlowWriter, Duration::from_millis(10));

        let err = writer.write(&ev()).await.unwrap_err();

        assert!(matches!(err, Error::Timeout(_)));
    }

    #[tokio::test]
    async fn timeout_writer_times_out_slow_write_all() {
        let writer = TimeoutWriter::new(SlowWriter, Duration::from_millis(10));

        let err = writer.write_all(&[ev(), ev()]).await.unwrap_err();

        assert!(matches!(err, Error::Timeout(_)));
    }

    #[tokio::test]
    async fn timeout_writer_forwards_inner_error() {
        struct Failing;
        impl Writer for Failing {
            async fn write(&self, _: &Event) -> Result<()> {
                Err(Error::Store("boom".to_owned()))
            }
        }
        let writer = TimeoutWriter::new(Failing, Duration::from_secs(1));

        let err = writer.write(&ev()).await.unwrap_err();
        assert!(err.to_string().contains("boom"));
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    struct TypedOk;

    impl Writer<UserUpdated> for TypedOk {
        async fn write(&self, _: &Event<UserUpdated>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn timeout_writer_supports_typed_payloads() {
        let writer = TimeoutWriter::new(TypedOk, Duration::from_secs(1));
        let event = Event::create(
            "org",
            "/users",
            "user.updated",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap();
        writer.write(&event).await.unwrap();
    }
}
