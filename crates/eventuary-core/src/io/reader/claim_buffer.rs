use std::future::Future;
use std::time::Duration;

use crate::error::Result;
use crate::event::Event;
use crate::io::OwnerId;

#[derive(Debug, Clone)]
pub struct ClaimedBufferEntry<Id> {
    pub id: Id,
    pub event: Event,
    pub attempts: u32,
}

pub trait ClaimedBufferStore: Clone + Send + Sync + 'static {
    type Id: Clone + Send + Sync + 'static;

    fn push<'a>(&'a self, event: &'a Event) -> impl Future<Output = Result<Self::Id>> + Send + 'a;

    fn claim_batch<'a>(
        &'a self,
        owner_id: &'a OwnerId,
        max: usize,
        visibility: Duration,
    ) -> impl Future<Output = Result<Vec<ClaimedBufferEntry<Self::Id>>>> + Send + 'a;

    fn ack<'a>(&'a self, id: &'a Self::Id) -> impl Future<Output = Result<()>> + Send + 'a;

    fn nack<'a>(&'a self, id: &'a Self::Id) -> impl Future<Output = Result<()>> + Send + 'a;
}
