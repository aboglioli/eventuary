use std::time::Duration;

use eventuary_core::io::acker::AckBufferConfig;
use eventuary_core::{Error, Result};

use crate::sqs::queue::SqsQueueType;
use crate::sqs::subscription::{MAX_MESSAGES, SqsSubscription};

#[derive(Debug, Clone)]
pub struct SqsReaderConfig {
    pub queue_url: String,
    pub queue_type: SqsQueueType,
    pub max_messages: i32,
    pub visibility_timeout: Duration,
    pub wait_time: Duration,
    pub ack_buffer: AckBufferConfig,
    pub limit: Option<usize>,
}

impl SqsReaderConfig {
    pub fn defaults_for(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            queue_type: SqsQueueType::default(),
            max_messages: 10,
            visibility_timeout: Duration::from_secs(30),
            wait_time: Duration::from_secs(20),
            ack_buffer: AckBufferConfig {
                max_pending: 10,
                flush_interval: Duration::from_secs(1),
            },
            limit: None,
        }
    }

    pub fn subscription(&self) -> SqsSubscription {
        SqsSubscription {
            queue_url: self.queue_url.clone(),
            queue_type: self.queue_type,
            wait_time: self.wait_time,
            visibility_timeout: self.visibility_timeout,
            max_messages: self.max_messages,
            limit: self.limit,
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.subscription().validate()?;
        if self.ack_buffer.max_pending == 0 || self.ack_buffer.max_pending > MAX_MESSAGES as usize {
            return Err(Error::Config(format!(
                "ack_buffer.max_pending must be 1..={MAX_MESSAGES} for SQS"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(n: usize) -> AckBufferConfig {
        AckBufferConfig {
            max_pending: n,
            flush_interval: Duration::from_secs(1),
        }
    }

    fn base() -> SqsReaderConfig {
        SqsReaderConfig::defaults_for("q")
    }

    #[test]
    fn defaults_ok() {
        base().validate().unwrap();
    }

    #[test]
    fn rejects_max_messages_above_10() {
        let mut c = base();
        c.max_messages = 11;
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_max_messages_zero() {
        let mut c = base();
        c.max_messages = 0;
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_wait_time_above_20s() {
        let mut c = base();
        c.wait_time = Duration::from_secs(21);
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_visibility_above_12h() {
        let mut c = base();
        c.visibility_timeout = Duration::from_secs(43_201);
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn accepts_a_limit() {
        let mut c = base();
        c.limit = Some(5);
        c.validate().unwrap();
    }

    #[test]
    fn rejects_ack_buffer_above_10() {
        let mut c = base();
        c.ack_buffer = ack(11);
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}
