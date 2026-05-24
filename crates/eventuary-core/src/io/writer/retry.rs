use std::time::Duration;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Writer;

#[derive(Debug, Clone)]
pub struct RetryWriterConfig {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl RetryWriterConfig {
    pub fn new(
        max_attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
        multiplier: f64,
    ) -> Result<Self> {
        if max_attempts == 0 {
            return Err(Error::Config(
                "retry writer max_attempts must be greater than zero".to_owned(),
            ));
        }
        if !multiplier.is_finite() || multiplier < 1.0 {
            return Err(Error::Config(
                "retry writer multiplier must be finite and >= 1.0".to_owned(),
            ));
        }
        Ok(Self {
            max_attempts,
            base_delay,
            max_delay,
            multiplier,
        })
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn base_delay(&self) -> Duration {
        self.base_delay
    }

    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }
}

impl Default for RetryWriterConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

pub struct RetryWriter<W> {
    inner: W,
    config: RetryWriterConfig,
}

impl<W> RetryWriter<W> {
    pub fn new(inner: W, config: RetryWriterConfig) -> Self {
        Self { inner, config }
    }

    pub fn config(&self) -> &RetryWriterConfig {
        &self.config
    }
}

impl<W, P> Writer<P> for RetryWriter<W>
where
    W: Writer<P>,
    P: Send + Sync,
{
    async fn write(&self, event: &Event<P>) -> Result<()> {
        let mut attempt = 1;
        loop {
            match self.inner.write(event).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt >= self.config.max_attempts => return Err(error),
                Err(_) => {
                    tokio::time::sleep(retry_delay(&self.config, attempt)).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn write_all(&self, events: &[Event<P>]) -> Result<()> {
        let mut attempt = 1;
        loop {
            match self.inner.write_all(events).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt >= self.config.max_attempts => return Err(error),
                Err(_) => {
                    tokio::time::sleep(retry_delay(&self.config, attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

fn retry_delay(config: &RetryWriterConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1);
    let delay = config.base_delay.as_secs_f64() * config.multiplier.powi(exponent as i32);
    Duration::from_secs_f64(delay.min(config.max_delay.as_secs_f64()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::payload::Payload;

    struct FlakyWriter {
        failures_remaining: Arc<Mutex<usize>>,
        attempts: Arc<AtomicUsize>,
    }

    impl Writer for FlakyWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let mut remaining = self.failures_remaining.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(Error::Store("temporary write failure".to_owned()));
            }
            Ok(())
        }

        async fn write_all(&self, _: &[Event]) -> Result<()> {
            self.write(&ev()).await
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    fn fast_config(max_attempts: u32) -> RetryWriterConfig {
        RetryWriterConfig::new(
            max_attempts,
            Duration::from_millis(1),
            Duration::from_millis(2),
            1.0,
        )
        .unwrap()
    }

    #[test]
    fn retry_writer_config_rejects_zero_attempts() {
        let err = RetryWriterConfig::new(0, Duration::ZERO, Duration::ZERO, 1.0).unwrap_err();

        assert!(err.to_string().contains("max_attempts"));
    }

    #[test]
    fn retry_writer_config_rejects_invalid_multiplier() {
        let err = RetryWriterConfig::new(1, Duration::ZERO, Duration::ZERO, 0.5).unwrap_err();

        assert!(err.to_string().contains("multiplier"));
    }

    #[test]
    fn retry_writer_config_default_is_valid() {
        let config = RetryWriterConfig::default();
        assert_eq!(config.max_attempts(), 3);
        assert_eq!(config.multiplier(), 2.0);
    }

    #[tokio::test]
    async fn retry_writer_retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let writer = RetryWriter::new(
            FlakyWriter {
                failures_remaining: Arc::new(Mutex::new(2)),
                attempts: Arc::clone(&attempts),
            },
            fast_config(3),
        );

        writer.write(&ev()).await.unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_writer_returns_last_error_after_exhaustion() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let writer = RetryWriter::new(
            FlakyWriter {
                failures_remaining: Arc::new(Mutex::new(5)),
                attempts: Arc::clone(&attempts),
            },
            fast_config(3),
        );

        let err = writer.write(&ev()).await.unwrap_err();

        assert!(err.to_string().contains("temporary write failure"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_writer_retries_write_all() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let writer = RetryWriter::new(
            FlakyWriter {
                failures_remaining: Arc::new(Mutex::new(1)),
                attempts: Arc::clone(&attempts),
            },
            fast_config(2),
        );

        writer.write_all(&[ev(), ev()]).await.unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    struct TypedFlakyWriter {
        attempts: Arc<AtomicUsize>,
    }

    impl Writer<UserUpdated> for TypedFlakyWriter {
        async fn write(&self, _: &Event<UserUpdated>) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(Error::Store("temporary".to_owned()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn retry_writer_supports_typed_payloads() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let writer = RetryWriter::new(
            TypedFlakyWriter {
                attempts: Arc::clone(&attempts),
            },
            fast_config(2),
        );
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

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
