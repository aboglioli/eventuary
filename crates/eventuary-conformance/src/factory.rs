use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use eventuary_core::{
    BoxWriter, ConsumerGroupId, Event, Namespace, OrganizationId, PartitionAssignment, Result,
    StartFrom, Topic,
};

use crate::Capabilities;

pub struct ReaderRequest {
    pub organization: Option<OrganizationId>,
    pub namespace: Option<Namespace>,
    pub topics: Vec<Topic>,
    pub consumer_group_id: Option<ConsumerGroupId>,
    pub checkpoint_name: String,
    pub start_from: StartFrom,
    pub poll_interval: Duration,
    pub partition: Option<PartitionAssignment>,
}

impl Default for ReaderRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl ReaderRequest {
    pub fn new() -> Self {
        Self {
            organization: None,
            namespace: None,
            topics: Vec::new(),
            consumer_group_id: None,
            checkpoint_name: "default".to_owned(),
            start_from: StartFrom::Earliest,
            poll_interval: Duration::from_millis(20),
            partition: None,
        }
    }

    pub fn for_organization(organization: OrganizationId) -> Self {
        Self {
            organization: Some(organization),
            ..Self::new()
        }
    }
}

pub type AckFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub type AckFn = Box<dyn FnOnce() -> AckFuture + Send>;

pub struct ConsumerEvent {
    pub event: Event,
    pub ack: AckFn,
    pub nack: AckFn,
}

pub trait Backend: Send + Sync {
    fn capabilities(&self) -> Capabilities;

    fn writer<'a>(&'a self) -> Pin<Box<dyn Future<Output = BoxWriter> + Send + 'a>>;

    fn read_one<'a>(
        &'a self,
        request: ReaderRequest,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Option<ConsumerEvent>> + Send + 'a>>;

    fn read_many<'a>(
        &'a self,
        request: ReaderRequest,
        count: usize,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Vec<ConsumerEvent>> + Send + 'a>>;
}
