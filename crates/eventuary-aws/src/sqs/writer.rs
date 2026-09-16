use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, EventId, Result};

use crate::batch::{self, Batcher, FailedEntry};
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
    ) -> Result<SendMessageBatchRequestEntry> {
        let mut builder = SendMessageBatchRequestEntry::builder()
            .id(entry_id.to_string())
            .message_body(body);
        if let Some(group_id) = self.config.queue_type.message_group_id(event) {
            builder = builder.message_group_id(group_id);
        }
        if let Some(dedup_id) = self.config.queue_type.message_deduplication_id(event) {
            builder = builder.message_deduplication_id(dedup_id);
        }
        builder.build().map_err(|e| Error::Store(e.to_string()))
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
        Err(batch::failure_report(
            "send_message_batch",
            &reported,
            event_ids,
        ))
    }
}

impl Writer for SqsWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = batch::serialize_body(event)?;
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
        let mut pending = Batcher::new();
        for event in events {
            let body = batch::serialize_body(event)?;
            let body_len = body.len();
            if pending.would_exceed(body_len) {
                let (entries, event_ids) = pending.take();
                self.send_batch(entries, &event_ids).await?;
            }
            let entry = self.entry(event, body, pending.len())?;
            pending.push(entry, event.id(), body_len);
        }
        if !pending.is_empty() {
            let (entries, event_ids) = pending.take();
            self.send_batch(entries, &event_ids).await?;
        }
        Ok(())
    }
}
