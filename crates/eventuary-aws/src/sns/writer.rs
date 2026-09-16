use aws_sdk_sns::Client;
use aws_sdk_sns::types::PublishBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result, SerializedEvent};

const SNS_BATCH_MAX: usize = 10;
const SNS_PAYLOAD_MAX: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SnsTopicType {
    #[default]
    Standard,
    Fifo,
    FifoContentBasedDeduplication,
}

impl SnsTopicType {
    fn message_group_id(&self, event: &Event) -> Option<String> {
        match self {
            Self::Standard => None,
            Self::Fifo | Self::FifoContentBasedDeduplication => {
                Some(event.key().as_str().to_owned())
            }
        }
    }

    fn message_deduplication_id(&self, event: &Event) -> Option<String> {
        match self {
            Self::Fifo => Some(event.id().to_string()),
            Self::Standard | Self::FifoContentBasedDeduplication => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnsWriterConfig {
    pub topic_type: SnsTopicType,
}

pub struct SnsWriter {
    client: Client,
    topic_arn: String,
    config: SnsWriterConfig,
}

impl SnsWriter {
    pub fn new(client: Client, topic_arn: impl Into<String>) -> Self {
        Self::new_with_config(client, topic_arn, SnsWriterConfig::default())
    }

    pub fn new_with_config(
        client: Client,
        topic_arn: impl Into<String>,
        config: SnsWriterConfig,
    ) -> Self {
        Self {
            client,
            topic_arn: topic_arn.into(),
            config,
        }
    }

    pub fn topic_arn(&self) -> &str {
        &self.topic_arn
    }

    fn serialize_body(event: &Event) -> Result<String> {
        let s = SerializedEvent::from_event(event)?;
        let body = s.to_json_string()?;
        if body.len() > SNS_PAYLOAD_MAX {
            return Err(Error::InvalidPayload(format!(
                "event body {} bytes exceeds 256 KB",
                body.len()
            )));
        }
        Ok(body)
    }

    async fn publish_batch(&self, entries: Vec<PublishBatchRequestEntry>) -> Result<()> {
        let resp = self
            .client
            .publish_batch()
            .topic_arn(&self.topic_arn)
            .set_publish_batch_request_entries(Some(entries))
            .send()
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        let failed = resp.failed();
        if !failed.is_empty() {
            let details: Vec<String> = failed
                .iter()
                .map(|f| {
                    format!(
                        "id={} code={} sender_fault={} message={}",
                        f.id(),
                        f.code(),
                        f.sender_fault(),
                        f.message().unwrap_or("")
                    )
                })
                .collect();
            return Err(Error::Store(format!(
                "publish_batch had {} failed entries: {}",
                failed.len(),
                details.join("; ")
            )));
        }
        Ok(())
    }
}

impl Writer for SnsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = Self::serialize_body(event)?;
        let mut request = self
            .client
            .publish()
            .topic_arn(&self.topic_arn)
            .message(body);
        if let Some(group_id) = self.config.topic_type.message_group_id(event) {
            request = request.message_group_id(group_id);
        }
        if let Some(dedup_id) = self.config.topic_type.message_deduplication_id(event) {
            request = request.message_deduplication_id(dedup_id);
        }
        request
            .send()
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        let mut current: Vec<PublishBatchRequestEntry> = Vec::new();
        let mut current_bytes = 0usize;

        for (id_counter, event) in events.iter().enumerate() {
            let body = Self::serialize_body(event)?;
            let body_len = body.len();
            let would_overflow =
                current.len() == SNS_BATCH_MAX || (current_bytes + body_len) > SNS_PAYLOAD_MAX;
            if would_overflow {
                let drained = std::mem::take(&mut current);
                self.publish_batch(drained).await?;
                current_bytes = 0;
            }
            let mut builder = PublishBatchRequestEntry::builder()
                .id(id_counter.to_string())
                .message(body);
            if let Some(group_id) = self.config.topic_type.message_group_id(event) {
                builder = builder.message_group_id(group_id);
            }
            if let Some(dedup_id) = self.config.topic_type.message_deduplication_id(event) {
                builder = builder.message_deduplication_id(dedup_id);
            }
            let entry = builder.build().map_err(|e| Error::Store(e.to_string()))?;
            current.push(entry);
            current_bytes += body_len;
        }
        if !current.is_empty() {
            self.publish_batch(current).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::Payload;

    fn event(key: &str) -> Event {
        Event::create(
            "acme",
            "/orders",
            "order.placed",
            key,
            Payload::from_string("v"),
        )
        .expect("valid event")
    }

    #[test]
    fn standard_sets_no_fifo_attributes() {
        let topic_type = SnsTopicType::Standard;
        let event = event("order-1");
        assert!(topic_type.message_group_id(&event).is_none());
        assert!(topic_type.message_deduplication_id(&event).is_none());
    }

    #[test]
    fn fifo_groups_by_event_key_and_dedupes_by_event_id() {
        let topic_type = SnsTopicType::Fifo;
        let event = event("order-1");
        assert_eq!(
            topic_type.message_group_id(&event).as_deref(),
            Some("order-1")
        );
        let expected_dedup_id = event.id().to_string();
        assert_eq!(
            topic_type.message_deduplication_id(&event).as_deref(),
            Some(expected_dedup_id.as_str())
        );
    }

    #[test]
    fn fifo_content_based_dedup_omits_deduplication_id() {
        let topic_type = SnsTopicType::FifoContentBasedDeduplication;
        let event = event("order-1");
        assert_eq!(
            topic_type.message_group_id(&event).as_deref(),
            Some("order-1")
        );
        assert!(topic_type.message_deduplication_id(&event).is_none());
    }

    #[test]
    fn events_sharing_a_key_share_a_message_group() {
        let topic_type = SnsTopicType::Fifo;
        let first = event("order-1");
        let second = event("order-1");
        assert_eq!(
            topic_type.message_group_id(&first),
            topic_type.message_group_id(&second)
        );
        assert_ne!(
            topic_type.message_deduplication_id(&first),
            topic_type.message_deduplication_id(&second)
        );
    }

    #[test]
    fn oversized_payload_is_rejected() {
        let big = "x".repeat(SNS_PAYLOAD_MAX + 1);
        let event = Event::create(
            "acme",
            "/orders",
            "order.placed",
            "order-1",
            Payload::from_string(big),
        )
        .expect("valid event");
        let err = SnsWriter::serialize_body(&event).unwrap_err();
        assert!(matches!(err, Error::InvalidPayload(_)));
    }

    #[test]
    fn serialize_body_round_trips_through_the_wire_format() {
        let event = event("order-1");
        let body = SnsWriter::serialize_body(&event).unwrap();
        let decoded = SerializedEvent::from_json_str(&body)
            .unwrap()
            .to_event()
            .unwrap();
        assert_eq!(decoded.id(), event.id());
        assert_eq!(decoded.key().as_str(), "order-1");
        assert_eq!(decoded.topic().as_str(), "order.placed");
    }
}
