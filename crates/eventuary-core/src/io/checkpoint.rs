use std::future::Future;

use crate::ConsumerGroupId;
use crate::error::{Error, Result};
use crate::partition::LogicalPartition;
use crate::start_from::StartFrom;

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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointResumePoint<C> {
    partition: Option<LogicalPartition>,
    cursor: C,
}

impl<C> CheckpointResumePoint<C> {
    pub fn new(partition: Option<LogicalPartition>, cursor: C) -> Self {
        Self { partition, cursor }
    }

    pub fn partition(&self) -> Option<LogicalPartition> {
        self.partition
    }

    pub fn cursor(&self) -> &C {
        &self.cursor
    }

    pub fn into_parts(self) -> (Option<LogicalPartition>, C) {
        (self.partition, self.cursor)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointResume<C> {
    start: StartFrom<C>,
    points: Vec<CheckpointResumePoint<C>>,
}

impl<C> CheckpointResume<C> {
    pub fn new(start: StartFrom<C>, points: Vec<CheckpointResumePoint<C>>) -> Self {
        Self { start, points }
    }

    pub fn start(&self) -> &StartFrom<C> {
        &self.start
    }

    pub fn points(&self) -> &[CheckpointResumePoint<C>] {
        &self.points
    }

    pub fn into_parts(self) -> (StartFrom<C>, Vec<CheckpointResumePoint<C>>) {
        (self.start, self.points)
    }
}

pub trait CheckpointResumableSubscription<C>: Clone + Send + 'static {
    fn with_checkpoint_resume(self, resume: CheckpointResume<C>) -> Self;
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
    use crate::io::NoCursor;
    use crate::start_from::StartFrom;

    #[test]
    fn stream_id_validation() {
        assert!(StreamId::new("billing").is_ok());
        assert!(StreamId::new("billing.invoices").is_ok());
        assert!(StreamId::new("").is_err());
        assert!(StreamId::new("bad space").is_err());
    }

    #[test]
    fn checkpoint_resume_preserves_start_and_points() {
        let point = CheckpointResumePoint::new(None, 42_i64);
        let resume = CheckpointResume::new(StartFrom::After(42_i64), vec![point.clone()]);

        assert_eq!(resume.start(), &StartFrom::After(42_i64));
        assert_eq!(resume.points(), &[point]);
    }

    #[test]
    fn checkpoint_resume_point_returns_parts() {
        let partition = LogicalPartition::new(1, std::num::NonZeroU16::new(4).unwrap()).unwrap();
        let point = CheckpointResumePoint::new(Some(partition), 99_i64);

        assert_eq!(point.partition(), Some(partition));
        assert_eq!(*point.cursor(), 99_i64);
        assert_eq!(point.into_parts(), (Some(partition), 99_i64));
    }

    #[test]
    fn checkpoint_resume_returns_parts() {
        let point = CheckpointResumePoint::new(None, 7_i64);
        let resume = CheckpointResume::new(StartFrom::After(7_i64), vec![point.clone()]);

        let (start, points) = resume.into_parts();

        assert_eq!(start, StartFrom::After(7_i64));
        assert_eq!(points, vec![point]);
    }

    #[test]
    fn checkpoint_resume_policy_defaults_to_use_initial_start() {
        assert_eq!(
            CheckpointResumePolicy::default(),
            CheckpointResumePolicy::UseInitialStart
        );
    }

    #[test]
    fn checkpoint_resume_supports_nocursor_for_cursorless_readers() {
        let resume = CheckpointResume::new(
            StartFrom::After(NoCursor),
            vec![CheckpointResumePoint::new(None, NoCursor)],
        );

        assert_eq!(resume.points().len(), 1);
    }
}
