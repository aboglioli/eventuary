use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::{Handler, Writer};
use crate::payload::Payload;
use crate::serialization::SerializedEvent;
use crate::topic::Topic;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryAction {
    Retry,
    DeadLetter(String),
    Skip,
}

pub trait RetryPolicy: Send + Sync {
    fn classify(&self, error: &Error, attempt: u32, max_attempts: u32) -> RetryAction;
}

pub struct DefaultRetryPolicy;

impl RetryPolicy for DefaultRetryPolicy {
    fn classify(&self, error: &Error, attempt: u32, max_attempts: u32) -> RetryAction {
        if attempt >= max_attempts {
            return RetryAction::DeadLetter(error.to_string());
        }
        RetryAction::Retry
    }
}

pub fn backoff_delay(config: &RetryConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1);
    let base = config.base_delay.as_secs_f64();
    let delay = base * config.multiplier.powi(exponent as i32);
    let max = config.max_delay.as_secs_f64();
    let capped = delay.min(max);
    Duration::from_secs_f64(capped)
}

pub struct DeadLetterWriter<W: Writer> {
    writer: W,
}

impl<W: Writer> DeadLetterWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub async fn send(
        &self,
        event: &Event,
        handler_id: &str,
        attempts: u32,
        reason: &str,
    ) -> Result<()> {
        let original = SerializedEvent::from_event(event)?.to_json_value();
        let dead_letter_payload = json!({
            "original_event": original,
            "original_event_id": event.id().to_string(),
            "handler_id": handler_id,
            "attempts": attempts,
            "error": reason,
            "failed_at": Utc::now(),
        });

        let dead_letter_topic = Topic::new(format!("{}.dead_letter", event.topic().as_str()))?;
        let mut builder = Event::builder(
            event.organization().as_str(),
            event.namespace().as_str(),
            dead_letter_topic.as_str(),
            Payload::from_json(&dead_letter_payload)?,
        )?;
        if let Some(key) = event.key() {
            builder = builder.key(key.as_str())?;
        }
        if let Some(correlation_id) = event.correlation_id() {
            builder = builder.correlation_id(correlation_id.as_str())?;
        }
        let dead_letter_event = builder.parent_id(event.id()).build();
        self.writer.write(&dead_letter_event).await
    }
}

pub struct RetryHandler<H: Handler, P: RetryPolicy, W: Writer> {
    inner: H,
    policy: P,
    config: RetryConfig,
    dead_letter: DeadLetterWriter<W>,
}

impl<H: Handler, P: RetryPolicy, W: Writer> RetryHandler<H, P, W> {
    pub fn new(inner: H, policy: P, config: RetryConfig, dead_letter: DeadLetterWriter<W>) -> Self {
        Self {
            inner,
            policy,
            config,
            dead_letter,
        }
    }
}

impl<H: Handler, P: RetryPolicy, W: Writer> Handler for RetryHandler<H, P, W> {
    fn id(&self) -> &str {
        self.inner.id()
    }

    async fn handle(&self, event: Event) -> Result<()> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = self.inner.handle(event.clone()).await;
            let error = match result {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };

            let action = self
                .policy
                .classify(&error, attempt, self.config.max_attempts);
            match action {
                RetryAction::Retry => {
                    if attempt >= self.config.max_attempts {
                        let reason = error.to_string();
                        self.dead_letter
                            .send(&event, self.inner.id(), attempt, &reason)
                            .await?;
                        return Ok(());
                    }
                    let delay = backoff_delay(&self.config, attempt);
                    tokio::time::sleep(delay).await;
                }
                RetryAction::DeadLetter(reason) => {
                    self.dead_letter
                        .send(&event, self.inner.id(), attempt, &reason)
                        .await?;
                    return Ok(());
                }
                RetryAction::Skip => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::payload::Payload;

    fn make_event() -> Event {
        Event::builder("acme", "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key("k")
            .unwrap()
            .build()
    }

    struct CapturingWriter {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl CapturingWriter {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn shared(&self) -> Arc<Mutex<Vec<Event>>> {
            Arc::clone(&self.events)
        }
    }

    impl Writer for CapturingWriter {
        async fn write(&self, event: &Event) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct FlakyHandler {
        id: String,
        fail_until: u32,
        attempts: Arc<AtomicUsize>,
    }

    impl Handler for FlakyHandler {
        fn id(&self) -> &str {
            &self.id
        }

        async fn handle(&self, _: Event) -> Result<()> {
            let count = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if (count as u32) <= self.fail_until {
                return Err(Error::Store(format!("attempt {count} failed")));
            }
            Ok(())
        }
    }

    #[test]
    fn backoff_delay_grows_exponentially() {
        let config = RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert_eq!(backoff_delay(&config, 1), Duration::from_millis(100));
        assert_eq!(backoff_delay(&config, 2), Duration::from_millis(200));
        assert_eq!(backoff_delay(&config, 3), Duration::from_millis(400));
    }

    #[test]
    fn backoff_delay_capped_at_max() {
        let config = RetryConfig {
            max_attempts: 20,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            multiplier: 2.0,
        };
        assert_eq!(backoff_delay(&config, 10), Duration::from_millis(500));
    }

    #[tokio::test]
    async fn retry_handler_retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let inner = FlakyHandler {
            id: "h".to_owned(),
            fail_until: 2,
            attempts: Arc::clone(&attempts),
        };
        let writer = CapturingWriter::new();
        let written = writer.shared();
        let retry = RetryHandler::new(
            inner,
            DefaultRetryPolicy,
            RetryConfig {
                max_attempts: 5,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(10),
                multiplier: 2.0,
            },
            DeadLetterWriter::new(writer),
        );

        retry.handle(make_event()).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(written.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn retry_handler_writes_dead_letter_after_max_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let inner = FlakyHandler {
            id: "h".to_owned(),
            fail_until: u32::MAX,
            attempts: Arc::clone(&attempts),
        };
        let writer = CapturingWriter::new();
        let written = writer.shared();
        let retry = RetryHandler::new(
            inner,
            DefaultRetryPolicy,
            RetryConfig {
                max_attempts: 3,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(10),
                multiplier: 2.0,
            },
            DeadLetterWriter::new(writer),
        );

        retry.handle(make_event()).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let captured = written.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].topic().as_str(), "thing.happened.dead_letter");
    }

    #[tokio::test]
    async fn dead_letter_payload_contains_original_event_and_handler_context() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let inner = FlakyHandler {
            id: "billing-handler".to_owned(),
            fail_until: u32::MAX,
            attempts: Arc::clone(&attempts),
        };
        let writer = CapturingWriter::new();
        let written = writer.shared();
        let original = make_event();
        let original_id = original.id();
        let retry = RetryHandler::new(
            inner,
            DefaultRetryPolicy,
            RetryConfig {
                max_attempts: 2,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(10),
                multiplier: 2.0,
            },
            DeadLetterWriter::new(writer),
        );

        retry.handle(original).await.unwrap();
        let captured = written.lock().unwrap();
        let dead_letter = &captured[0];
        let value: serde_json::Value = dead_letter.payload().to_json().unwrap();
        assert_eq!(
            value["original_event_id"].as_str().unwrap(),
            original_id.to_string()
        );
        assert_eq!(value["handler_id"].as_str().unwrap(), "billing-handler");
        assert_eq!(value["attempts"].as_u64().unwrap(), 2);
        assert!(value["original_event"].is_object());
        assert!(value["error"].is_string());
        assert!(value["failed_at"].is_string());
    }
}
