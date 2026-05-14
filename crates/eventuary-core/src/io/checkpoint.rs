use std::future::Future;

use crate::ConsumerGroupId;
use crate::error::{Error, Result};
use crate::partition::LogicalPartition;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StreamId(String);

impl StreamId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "invalid stream id length: {:?}",
                value
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(Error::Config(format!("invalid stream id: {value:?}")));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CheckpointScope {
    pub consumer_group_id: ConsumerGroupId,
    pub stream_id: StreamId,
}

impl CheckpointScope {
    pub fn new(consumer_group_id: ConsumerGroupId, stream_id: StreamId) -> Self {
        Self {
            consumer_group_id,
            stream_id,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CheckpointKey {
    pub scope: CheckpointScope,
    pub partition: Option<LogicalPartition>,
}

impl CheckpointKey {
    pub fn new(scope: CheckpointScope, partition: Option<LogicalPartition>) -> Self {
        Self { scope, partition }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum CheckpointResumePolicy {
    #[default]
    UseInitialStart,
    Error,
}

pub trait CheckpointStore<C>: Clone + Send + Sync + 'static {
    fn load<'a>(
        &'a self,
        key: &'a CheckpointKey,
    ) -> impl Future<Output = Result<Option<C>>> + Send + 'a;

    fn load_scope<'a>(
        &'a self,
        scope: &'a CheckpointScope,
    ) -> impl Future<Output = Result<Vec<(Option<LogicalPartition>, C)>>> + Send + 'a;

    fn commit<'a>(
        &'a self,
        key: &'a CheckpointKey,
        cursor: C,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_validation() {
        assert!(StreamId::new("billing").is_ok());
        assert!(StreamId::new("billing.invoices").is_ok());
        assert!(StreamId::new("").is_err());
        assert!(StreamId::new("bad space").is_err());
    }

    #[test]
    fn checkpoint_resume_policy_defaults_to_use_initial_start() {
        assert_eq!(
            CheckpointResumePolicy::default(),
            CheckpointResumePolicy::UseInitialStart
        );
    }
}
