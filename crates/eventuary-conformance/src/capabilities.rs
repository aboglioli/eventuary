/// Backend capability flags used by the conformance suite to gate which
/// cases run against a given backend.
///
/// Names reflect the cursor-reader composition: backends advertise which
/// composers and cursor behaviors they support.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    /// Reader emits a non-`NoCursor` delivery cursor.
    pub supports_cursor: bool,
    /// Backend can resume from `StartFrom::After(cursor)`.
    pub supports_start_after: bool,
    /// Backend can re-deliver an event after a downstream `nack`.
    pub supports_nack_redelivery: bool,
    /// Backend has a `CheckpointStore` implementation usable with
    /// `CheckpointReader`.
    pub supports_checkpoint_store: bool,
    /// Backend reader composes with `PartitionedReader` (cursor is
    /// `Clone + Send + Sync`).
    pub supports_partitioned_reader_composition: bool,
    /// Backend reader preserves the total event order within a partition.
    pub preserves_total_order: bool,
    /// Backend honors `StartFrom::Timestamp(_)`.
    pub supports_timestamp_start: bool,
}

impl Capabilities {
    pub fn full() -> Self {
        Self {
            supports_cursor: true,
            supports_start_after: true,
            supports_nack_redelivery: true,
            supports_checkpoint_store: true,
            supports_partitioned_reader_composition: true,
            preserves_total_order: true,
            supports_timestamp_start: true,
        }
    }

    pub fn minimal() -> Self {
        Self {
            supports_cursor: false,
            supports_start_after: false,
            supports_nack_redelivery: false,
            supports_checkpoint_store: false,
            supports_partitioned_reader_composition: false,
            preserves_total_order: false,
            supports_timestamp_start: false,
        }
    }
}
