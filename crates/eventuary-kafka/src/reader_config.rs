use std::time::Duration;

use chrono::{DateTime, Utc};

use eventuary_core::io::acker::AckBufferConfig;
use eventuary_core::{ConsumerGroupId, Error, Namespace, OrganizationId, Result, StartFrom, Topic};

#[derive(Debug, Clone)]
pub struct KafkaReaderConfig {
    pub brokers: Vec<String>,
    pub kafka_topics: Vec<String>,
    pub consumer_group_id: ConsumerGroupId,
    pub organization: OrganizationId,
    pub start_from: StartFrom,
    pub event_topics: Option<Vec<Topic>>,
    pub namespace: Option<Namespace>,
    pub session_timeout: Duration,
    pub max_poll_records: usize,
    pub end_at: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub ack_buffer: AckBufferConfig,
}

impl KafkaReaderConfig {
    pub fn new(
        brokers: Vec<String>,
        kafka_topics: Vec<String>,
        consumer_group_id: ConsumerGroupId,
        organization: OrganizationId,
    ) -> Result<Self> {
        Self::validated(Self::streaming(
            brokers,
            kafka_topics,
            consumer_group_id,
            organization,
        ))
    }

    pub fn streaming(
        brokers: Vec<String>,
        kafka_topics: Vec<String>,
        consumer_group_id: ConsumerGroupId,
        organization: OrganizationId,
    ) -> Self {
        Self {
            brokers,
            kafka_topics,
            consumer_group_id,
            organization,
            start_from: StartFrom::Latest,
            event_topics: None,
            namespace: None,
            session_timeout: Duration::from_secs(30),
            max_poll_records: 100,
            end_at: None,
            limit: None,
            ack_buffer: AckBufferConfig::default(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.brokers.is_empty() {
            return Err(Error::Config("brokers must not be empty".to_owned()));
        }
        if self.brokers.iter().any(|b| b.trim().is_empty()) {
            return Err(Error::Config("broker entries must not be blank".to_owned()));
        }
        if self.kafka_topics.is_empty() {
            return Err(Error::Config("kafka_topics must not be empty".to_owned()));
        }
        if self.kafka_topics.iter().any(|t| t.trim().is_empty()) {
            return Err(Error::Config(
                "kafka topic names must not be blank".to_owned(),
            ));
        }
        if self.max_poll_records == 0 {
            return Err(Error::Config("max_poll_records must be > 0".to_owned()));
        }
        if self.ack_buffer.max_pending == 0 {
            return Err(Error::Config(
                "ack_buffer.max_pending must be > 0".to_owned(),
            ));
        }
        Ok(())
    }

    fn validated(cfg: Self) -> Result<Self> {
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> KafkaReaderConfig {
        KafkaReaderConfig::streaming(
            vec!["broker:9092".to_owned()],
            vec!["t".to_owned()],
            ConsumerGroupId::new("g").unwrap(),
            OrganizationId::new("o").unwrap(),
        )
    }

    #[test]
    fn streaming_defaults() {
        let c = cfg();
        assert_eq!(c.kafka_topics.len(), 1);
        assert!(matches!(c.start_from, StartFrom::Latest));
        assert!(c.event_topics.is_none());
        assert!(c.namespace.is_none());
    }

    #[test]
    fn validate_ok() {
        cfg().validate().unwrap();
    }

    #[test]
    fn rejects_empty_brokers() {
        let mut c = cfg();
        c.brokers.clear();
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_blank_broker() {
        let mut c = cfg();
        c.brokers = vec!["   ".to_owned()];
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_empty_topics() {
        let mut c = cfg();
        c.kafka_topics.clear();
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_blank_topic() {
        let mut c = cfg();
        c.kafka_topics = vec!["".to_owned()];
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_zero_max_poll_records() {
        let mut c = cfg();
        c.max_poll_records = 0;
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_zero_ack_buffer() {
        let mut c = cfg();
        c.ack_buffer = AckBufferConfig {
            max_pending: 0,
            flush_interval: Duration::from_secs(1),
        };
        let err = c.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn new_returns_validated_config() {
        let err = KafkaReaderConfig::new(
            Vec::new(),
            vec!["t".to_owned()],
            ConsumerGroupId::new("g").unwrap(),
            OrganizationId::new("o").unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}
