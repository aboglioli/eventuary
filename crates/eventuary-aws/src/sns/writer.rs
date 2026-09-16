use aws_sdk_sns::Client;
use aws_sdk_sns::types::PublishBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, EventId, Result, SerializedEvent};

use crate::sns::topic::SnsTopicType;

const SNS_BATCH_MAX: usize = 10;
const SNS_PAYLOAD_MAX: usize = 256 * 1024;

fn would_exceed_batch_limits(entry_count: usize, batch_bytes: usize, next_body: usize) -> bool {
    entry_count == SNS_BATCH_MAX || batch_bytes + next_body > SNS_PAYLOAD_MAX
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

    async fn publish_batch(
        &self,
        entries: Vec<PublishBatchRequestEntry>,
        event_ids: &[EventId],
    ) -> Result<()> {
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
                    let event = f
                        .id()
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| event_ids.get(i))
                        .map(EventId::to_string)
                        .unwrap_or_else(|| f.id().to_owned());
                    format!(
                        "event={} code={} sender_fault={} message={}",
                        event,
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
        let mut current_event_ids: Vec<EventId> = Vec::new();
        let mut current_bytes = 0usize;

        for event in events {
            let body = Self::serialize_body(event)?;
            let body_len = body.len();
            if would_exceed_batch_limits(current.len(), current_bytes, body_len) {
                let drained = std::mem::take(&mut current);
                let drained_ids = std::mem::take(&mut current_event_ids);
                self.publish_batch(drained, &drained_ids).await?;
                current_bytes = 0;
            }
            let mut builder = PublishBatchRequestEntry::builder()
                .id(current.len().to_string())
                .message(body);
            if let Some(group_id) = self.config.topic_type.message_group_id(event) {
                builder = builder.message_group_id(group_id);
            }
            if let Some(dedup_id) = self.config.topic_type.message_deduplication_id(event) {
                builder = builder.message_deduplication_id(dedup_id);
            }
            let entry = builder.build().map_err(|e| Error::Store(e.to_string()))?;
            current.push(entry);
            current_event_ids.push(event.id());
            current_bytes += body_len;
        }
        if !current.is_empty() {
            self.publish_batch(current, &current_event_ids).await?;
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

    fn batch_sizes(event_count: usize, body_len: usize) -> Vec<usize> {
        let mut sizes = Vec::new();
        let mut entries = 0usize;
        let mut bytes = 0usize;
        for _ in 0..event_count {
            if would_exceed_batch_limits(entries, bytes, body_len) {
                sizes.push(entries);
                entries = 0;
                bytes = 0;
            }
            entries += 1;
            bytes += body_len;
        }
        if entries > 0 {
            sizes.push(entries);
        }
        sizes
    }

    #[test]
    fn flushes_only_once_ten_entries_are_buffered() {
        assert!(!would_exceed_batch_limits(SNS_BATCH_MAX - 1, 0, 1));
        assert!(would_exceed_batch_limits(SNS_BATCH_MAX, 0, 1));
    }

    #[test]
    fn flushes_before_the_payload_limit_is_crossed() {
        assert!(!would_exceed_batch_limits(1, SNS_PAYLOAD_MAX - 1, 1));
        assert!(would_exceed_batch_limits(1, SNS_PAYLOAD_MAX, 1));
    }

    #[test]
    fn no_batch_ever_exceeds_ten_entries() {
        for count in [0, 1, 9, 10, 11, 25, 100, 1001] {
            let sizes = batch_sizes(count, 16);
            assert!(
                sizes.iter().all(|n| *n <= SNS_BATCH_MAX),
                "count {count} produced {sizes:?}"
            );
            assert_eq!(sizes.iter().sum::<usize>(), count);
            assert!(sizes.iter().all(|n| *n > 0));
        }
    }

    #[test]
    fn exactly_ten_events_publish_as_one_batch() {
        assert_eq!(batch_sizes(10, 16), vec![10]);
        assert_eq!(batch_sizes(11, 16), vec![10, 1]);
        assert_eq!(batch_sizes(25, 16), vec![10, 10, 5]);
    }

    #[test]
    fn oversized_bodies_split_before_the_entry_limit() {
        let half = SNS_PAYLOAD_MAX / 2;
        assert_eq!(batch_sizes(4, half), vec![2, 2]);
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
