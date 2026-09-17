use aws_sdk_sns::Client;
use aws_sdk_sns::types::PublishBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result};

use crate::batch::{Batch, Batcher};
use crate::request::{self, Entry, FailedEntry};
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
    ) -> Result<Entry<PublishBatchRequestEntry>> {
        let mut builder = PublishBatchRequestEntry::builder()
            .id(entry_id.to_string())
            .message(body);
        if let Some(group_id) = self.config.topic_type.message_group_id(event) {
            builder = builder.message_group_id(group_id);
        }
        if let Some(dedup_id) = self.config.topic_type.message_deduplication_id(event) {
            builder = builder.message_deduplication_id(dedup_id);
        }
        let request = builder.build().map_err(|e| Error::Store(e.to_string()))?;
        Ok(Entry::new(request, event.id()))
    }

    async fn publish_batch(&self, sealed: Batch<Entry<PublishBatchRequestEntry>>) -> Result<()> {
        let (entries, event_ids) = request::unzip(sealed.into_items());
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
        Err(request::failure_report(
            "publish_batch",
            &reported,
            &event_ids,
        ))
    }
}

impl Writer for SnsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = request::serialize_body(event)?;
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
        let mut batcher = Batcher::new(request::BATCH_LIMITS);
        for event in events {
            let body = request::serialize_body(event)?;
            let weight = body.len();
            if let Some(sealed) = batcher.push(weight, |index| self.entry(event, body, index))? {
                self.publish_batch(sealed).await?;
            }
        }
        if let Some(tail) = batcher.finish() {
            self.publish_batch(tail).await?;
        }
        Ok(())
    }
}
