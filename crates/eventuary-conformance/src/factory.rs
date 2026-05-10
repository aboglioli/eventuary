use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use eventuary::{
    BoxWriter, ConsumerGroupId, Event, Namespace, OrganizationId, Result, StartFrom, Topic,
};

use crate::Capabilities;

pub struct ReaderRequest {
    pub organization: OrganizationId,
    pub namespace: Option<Namespace>,
    pub topics: Vec<Topic>,
    pub consumer_group_id: Option<ConsumerGroupId>,
    pub stream: String,
    pub start_from: StartFrom,
    pub poll_interval: Duration,
}

impl ReaderRequest {
    pub fn new(organization: OrganizationId) -> Self {
        Self {
            organization,
            namespace: None,
            topics: Vec::new(),
            consumer_group_id: None,
            stream: "default".to_owned(),
            start_from: StartFrom::Earliest,
            poll_interval: Duration::from_millis(20),
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
