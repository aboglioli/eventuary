use std::time::Duration;

use eventuary_core::io::ConsumerGroupId;
use eventuary_core::io::acker::AckBufferConfig;
use eventuary_core::{Error, Result, StartFrom};

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
    pub start_from: StartFrom,
    pub limit: Option<usize>,
    pub consumer_group_id: Option<ConsumerGroupId>,
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
            start_from: StartFrom::Latest,
            limit: None,
            consumer_group_id: None,
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
        if !matches!(self.start_from, StartFrom::Latest) {
            return Err(Error::Config(
                "SQS only supports StartFrom::Latest".to_owned(),
            ));
        }
        if self.limit.is_some() {
            return Err(Error::Config("SQS does not support limit".to_owned()));
        }
        if self.consumer_group_id.is_some() {
            return Err(Error::Config(
                "SQS uses queue URL as consumer identity; consumer_group_id is not supported"
                    .to_owned(),
            ));
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
    fn rejects_ack_buffer_above_10() {
        let mut c = base();
        c.ack_buffer = ack(11);
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_earliest() {
        let mut c = base();
        c.start_from = StartFrom::Earliest;
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_timestamp_start() {
        let mut c = base();
        c.start_from = StartFrom::Timestamp(chrono::Utc::now());
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_limit() {
        let mut c = base();
        c.limit = Some(5);
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_consumer_group_id() {
        let mut c = base();
        c.consumer_group_id = Some(ConsumerGroupId::new("g").unwrap());
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}
