#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub supports_replay: bool,
    pub supports_timestamp_start: bool,
    pub supports_nack_redelivery: bool,
    pub preserves_total_order: bool,
    pub supports_consumer_groups: bool,
    pub supports_independent_checkpoints: bool,
    /// Backend honors `subscription.partition` so N workers with distinct
    /// ids and the same count receive disjoint slices of the event log.
    pub supports_runtime_partitioning: bool,
}

impl Capabilities {
    pub fn full() -> Self {
        Self {
            supports_replay: true,
            supports_timestamp_start: true,
            supports_nack_redelivery: true,
            preserves_total_order: true,
            supports_consumer_groups: true,
            supports_independent_checkpoints: true,
            supports_runtime_partitioning: true,
        }
    }
}
