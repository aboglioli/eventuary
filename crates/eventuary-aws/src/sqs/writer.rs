use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, EventId, Result, SerializedEvent};

use crate::sqs::queue::SqsQueueType;

const SQS_BATCH_MAX: usize = 10;
const SQS_PAYLOAD_MAX: usize = 256 * 1024;

fn would_exceed_batch_limits(entry_count: usize, batch_bytes: usize, next_body: usize) -> bool {
    entry_count == SQS_BATCH_MAX || batch_bytes + next_body > SQS_PAYLOAD_MAX
}

#[derive(Debug, Clone, Default)]
pub struct SqsWriterConfig {
    pub queue_type: SqsQueueType,
}

pub struct SqsWriter {
    client: Client,
    queue_url: String,
    config: SqsWriterConfig,
}

impl SqsWriter {
    pub fn new(client: Client, queue_url: impl Into<String>) -> Self {
        Self::new_with_config(client, queue_url, SqsWriterConfig::default())
    }

    pub fn new_with_config(
        client: Client,
        queue_url: impl Into<String>,
        config: SqsWriterConfig,
    ) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
            config,
        }
    }

    pub fn queue_url(&self) -> &str {
        &self.queue_url
    }

    fn serialize_body(event: &Event) -> Result<String> {
        let s = SerializedEvent::from_event(event)?;
        let body = s.to_json_string()?;
        if body.len() > SQS_PAYLOAD_MAX {
            return Err(Error::InvalidPayload(format!(
                "event body {} bytes exceeds 256 KB",
                body.len()
            )));
        }
        Ok(body)
    }

    async fn send_batch(
        &self,
        entries: Vec<SendMessageBatchRequestEntry>,
        event_ids: &[EventId],
    ) -> Result<()> {
        let resp = self
            .client
            .send_message_batch()
            .queue_url(&self.queue_url)
            .set_entries(Some(entries))
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
                "send_message_batch had {} failed entries: {}",
                failed.len(),
                details.join("; ")
            )));
        }
        Ok(())
    }
}

impl Writer for SqsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = Self::serialize_body(event)?;
        let mut request = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(body);
        if let Some(group_id) = self.config.queue_type.message_group_id(event) {
            request = request.message_group_id(group_id);
        }
        if let Some(dedup_id) = self.config.queue_type.message_deduplication_id(event) {
            request = request.message_deduplication_id(dedup_id);
        }
        request
            .send()
            .await
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        let mut current: Vec<SendMessageBatchRequestEntry> = Vec::new();
        let mut current_event_ids: Vec<EventId> = Vec::new();
        let mut current_bytes = 0usize;

        for event in events {
            let body = Self::serialize_body(event)?;
            let body_len = body.len();
            if would_exceed_batch_limits(current.len(), current_bytes, body_len) {
                let drained = std::mem::take(&mut current);
                let drained_ids = std::mem::take(&mut current_event_ids);
                self.send_batch(drained, &drained_ids).await?;
                current_bytes = 0;
            }
            let mut builder = SendMessageBatchRequestEntry::builder()
                .id(current.len().to_string())
                .message_body(body);
            if let Some(group_id) = self.config.queue_type.message_group_id(event) {
                builder = builder.message_group_id(group_id);
            }
            if let Some(dedup_id) = self.config.queue_type.message_deduplication_id(event) {
                builder = builder.message_deduplication_id(dedup_id);
            }
            let entry = builder.build().map_err(|e| Error::Store(e.to_string()))?;
            current.push(entry);
            current_event_ids.push(event.id());
            current_bytes += body_len;
        }
        if !current.is_empty() {
            self.send_batch(current, &current_event_ids).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!would_exceed_batch_limits(SQS_BATCH_MAX - 1, 0, 1));
        assert!(would_exceed_batch_limits(SQS_BATCH_MAX, 0, 1));
    }

    #[test]
    fn flushes_before_the_payload_limit_is_crossed() {
        assert!(!would_exceed_batch_limits(1, SQS_PAYLOAD_MAX - 1, 1));
        assert!(would_exceed_batch_limits(1, SQS_PAYLOAD_MAX, 1));
    }

    #[test]
    fn no_batch_ever_exceeds_ten_entries() {
        for count in [0, 1, 9, 10, 11, 25, 100, 1001] {
            let sizes = batch_sizes(count, 16);
            assert!(
                sizes.iter().all(|n| *n <= SQS_BATCH_MAX),
                "count {count} produced {sizes:?}"
            );
            assert_eq!(sizes.iter().sum::<usize>(), count);
            assert!(sizes.iter().all(|n| *n > 0));
        }
    }

    #[test]
    fn exactly_ten_events_send_as_one_batch() {
        assert_eq!(batch_sizes(10, 16), vec![10]);
        assert_eq!(batch_sizes(11, 16), vec![10, 1]);
        assert_eq!(batch_sizes(25, 16), vec![10, 10, 5]);
    }

    #[test]
    fn oversized_bodies_split_before_the_entry_limit() {
        let half = SQS_PAYLOAD_MAX / 2;
        assert_eq!(batch_sizes(4, half), vec![2, 2]);
    }
}
