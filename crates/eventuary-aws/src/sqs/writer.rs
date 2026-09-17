use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result};

use crate::batch::{Batch, Batcher};
use crate::request::{self, Entry, FailedEntry};
use crate::sqs::queue::SqsQueueType;

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

    fn entry(
        &self,
        event: &Event,
        body: String,
        entry_id: usize,
    ) -> Result<Entry<SendMessageBatchRequestEntry>> {
        let mut builder = SendMessageBatchRequestEntry::builder()
            .id(entry_id.to_string())
            .message_body(body);
        if let Some(group_id) = self.config.queue_type.message_group_id(event) {
            builder = builder.message_group_id(group_id);
        }
        if let Some(dedup_id) = self.config.queue_type.message_deduplication_id(event) {
            builder = builder.message_deduplication_id(dedup_id);
        }
        let request = builder.build().map_err(|e| Error::Store(e.to_string()))?;
        Ok(Entry::new(request, event.id()))
    }

    async fn send_batch(&self, sealed: Batch<Entry<SendMessageBatchRequestEntry>>) -> Result<()> {
        let (entries, event_ids) = request::unzip(sealed.into_items());
        let resp = self
            .client
            .send_message_batch()
            .queue_url(&self.queue_url)
            .set_entries(Some(entries))
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
            "send_message_batch",
            &reported,
            &event_ids,
        ))
    }
}

impl Writer for SqsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = request::serialize_body(event)?;
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
        let mut batcher = Batcher::new(request::BATCH_LIMITS);
        for event in events {
            let body = request::serialize_body(event)?;
            let weight = body.len();
            if let Some(sealed) = batcher.push(weight, |index| self.entry(event, body, index))? {
                self.send_batch(sealed).await?;
            }
        }
        if let Some(tail) = batcher.finish() {
            self.send_batch(tail).await?;
        }
        Ok(())
    }
}
