use aws_sdk_sns::Client;
use aws_sdk_sns::types::PublishBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, EventId, Result};

use crate::batch::{self, Batcher, FailedEntry};
use crate::sns::topic::SnsTopicType;

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

    fn entry(
        &self,
        event: &Event,
        body: String,
        entry_id: usize,
    ) -> Result<PublishBatchRequestEntry> {
        let mut builder = PublishBatchRequestEntry::builder()
            .id(entry_id.to_string())
            .message(body);
        if let Some(group_id) = self.config.topic_type.message_group_id(event) {
            builder = builder.message_group_id(group_id);
        }
        if let Some(dedup_id) = self.config.topic_type.message_deduplication_id(event) {
            builder = builder.message_deduplication_id(dedup_id);
        }
        builder.build().map_err(|e| Error::Store(e.to_string()))
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
        if failed.is_empty() {
            return Ok(());
        }
        let reported: Vec<FailedEntry<'_>> = failed
            .iter()
            .map(|f| FailedEntry {
                id: f.id(),
                code: f.code(),
                sender_fault: f.sender_fault(),
                message: f.message(),
            })
            .collect();
        Err(batch::failure_report("publish_batch", &reported, event_ids))
    }
}

impl Writer for SnsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = batch::serialize_body(event)?;
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
        let mut pending = Batcher::new();
        for event in events {
            let body = batch::serialize_body(event)?;
            let body_len = body.len();
            if pending.would_exceed(body_len) {
                let (entries, event_ids) = pending.take();
                self.publish_batch(entries, &event_ids).await?;
            }
            let entry = self.entry(event, body, pending.len())?;
            pending.push(entry, event.id(), body_len);
        }
        if !pending.is_empty() {
            let (entries, event_ids) = pending.take();
            self.publish_batch(entries, &event_ids).await?;
        }
        Ok(())
    }
}
