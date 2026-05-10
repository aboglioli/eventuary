use std::time::Duration;

use eventuary::io::acker::AckBufferConfig;
use eventuary::{Error, Namespace, OrganizationId, Result, StartFrom, Topic};

const SQS_MAX_MESSAGES_LIMIT: usize = 10;
const SQS_MAX_WAIT_TIME: Duration = Duration::from_secs(20);
const SQS_MAX_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(43_200);

#[derive(Debug, Clone)]
pub struct SqsReaderConfig {
    pub queue_url: String,
    pub organization: OrganizationId,
    pub namespace: Option<Namespace>,
    pub topics: Vec<Topic>,
    pub max_messages: usize,
    pub visibility_timeout: Duration,
    pub wait_time: Duration,
    pub ack_buffer: AckBufferConfig,
}

pub struct SqsReaderParams {
    pub queue_url: String,
    pub organization: OrganizationId,
    pub namespace: Option<Namespace>,
    pub topics: Vec<Topic>,
    pub max_messages: usize,
    pub visibility_timeout: Duration,
    pub wait_time: Duration,
    pub ack_buffer: AckBufferConfig,
    pub start_from: StartFrom,
    pub end_at: Option<chrono::DateTime<chrono::Utc>>,
    pub limit: Option<usize>,
    pub consumer_group_id: Option<String>,
}

impl SqsReaderConfig {
    pub fn new(params: SqsReaderParams) -> Result<Self> {
        if !(1..=SQS_MAX_MESSAGES_LIMIT).contains(&params.max_messages) {
            return Err(Error::Config(format!(
                "max_messages must be 1..={SQS_MAX_MESSAGES_LIMIT}"
            )));
        }
        if params.wait_time > SQS_MAX_WAIT_TIME {
            return Err(Error::Config("wait_time max 20s".to_owned()));
        }
        if params.visibility_timeout > SQS_MAX_VISIBILITY_TIMEOUT {
            return Err(Error::Config("visibility_timeout max 12h".to_owned()));
        }
        if params.ack_buffer.max_pending == 0
            || params.ack_buffer.max_pending > SQS_MAX_MESSAGES_LIMIT
        {
            return Err(Error::Config(format!(
                "ack_buffer.max_pending must be 1..={SQS_MAX_MESSAGES_LIMIT} for SQS"
            )));
        }
        if !matches!(params.start_from, StartFrom::Latest) {
            return Err(Error::Config(
                "SQS only supports StartFrom::Latest".to_owned(),
            ));
        }
        if params.end_at.is_some() {
            return Err(Error::Config("SQS does not support end_at".to_owned()));
        }
        if params.limit.is_some() {
            return Err(Error::Config("SQS does not support limit".to_owned()));
        }
        if params.consumer_group_id.is_some() {
            return Err(Error::Config(
                "SQS uses queue URL as consumer identity; consumer_group_id is not supported"
                    .to_owned(),
            ));
        }
        Ok(Self {
            queue_url: params.queue_url,
            organization: params.organization,
            namespace: params.namespace,
            topics: params.topics,
            max_messages: params.max_messages,
            visibility_timeout: params.visibility_timeout,
            wait_time: params.wait_time,
            ack_buffer: params.ack_buffer,
        })
    }

    pub fn defaults_for(
        queue_url: impl Into<String>,
        organization: OrganizationId,
    ) -> Result<Self> {
        Self::new(SqsReaderParams {
            queue_url: queue_url.into(),
            organization,
            namespace: None,
            topics: Vec::new(),
            max_messages: 10,
            visibility_timeout: Duration::from_secs(30),
            wait_time: Duration::from_secs(20),
            ack_buffer: AckBufferConfig {
                max_pending: 10,
                flush_interval: Duration::from_secs(1),
            },
            start_from: StartFrom::Latest,
            end_at: None,
            limit: None,
            consumer_group_id: None,
        })
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

    fn org() -> OrganizationId {
        OrganizationId::new("o").unwrap()
    }

    fn base_params() -> SqsReaderParams {
        SqsReaderParams {
            queue_url: "q".to_owned(),
            organization: org(),
            namespace: None,
            topics: Vec::new(),
            max_messages: 10,
            visibility_timeout: Duration::from_secs(30),
            wait_time: Duration::from_secs(20),
            ack_buffer: ack(10),
            start_from: StartFrom::Latest,
            end_at: None,
            limit: None,
            consumer_group_id: None,
        }
    }

    #[test]
    fn defaults_ok() {
        SqsReaderConfig::defaults_for("https://q", org()).unwrap();
    }

    #[test]
    fn rejects_max_messages_above_10() {
        let mut p = base_params();
        p.max_messages = 11;
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_max_messages_zero() {
        let mut p = base_params();
        p.max_messages = 0;
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_wait_time_above_20s() {
        let mut p = base_params();
        p.wait_time = Duration::from_secs(21);
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_visibility_above_12h() {
        let mut p = base_params();
        p.visibility_timeout = Duration::from_secs(43_201);
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_ack_buffer_above_10() {
        let mut p = base_params();
        p.ack_buffer = ack(11);
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_earliest() {
        let mut p = base_params();
        p.start_from = StartFrom::Earliest;
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_timestamp_start() {
        let mut p = base_params();
        p.start_from = StartFrom::Timestamp(chrono::Utc::now());
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_end_at() {
        let mut p = base_params();
        p.end_at = Some(chrono::Utc::now());
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_limit() {
        let mut p = base_params();
        p.limit = Some(5);
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_consumer_group_id() {
        let mut p = base_params();
        p.consumer_group_id = Some("g".to_owned());
        let err = SqsReaderConfig::new(p).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}
