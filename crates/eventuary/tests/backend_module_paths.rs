#![cfg(all(
    feature = "memory",
    feature = "sqlite",
    feature = "postgres",
    feature = "sqs",
    feature = "kafka"
))]

#[test]
fn backend_types_are_available_at_role_module_paths() {
    fn assert_type<T>() {}

    assert_type::<eventuary::memory::reader::MemoryReader>();
    assert_type::<eventuary::memory::reader::MemorySubscription>();
    assert_type::<eventuary::memory::writer::MemoryWriter>();
    assert_type::<eventuary::memory::checkpoint::MemoryCheckpointStore<eventuary::io::NoCursor>>();
    assert_type::<eventuary::memory::coordinator::MemoryPartitionCoordinator<eventuary::io::NoCursor>>();
    assert_type::<eventuary::memory::buffer::MemoryBufferStore<eventuary::io::NoCursor>>();
    assert_type::<eventuary::memory::claim_buffer::MemoryClaimedBufferStore>();
    assert_type::<eventuary::memory::dedupe::MemoryDedupeStore>();
    assert_type::<eventuary::memory::multiplexer::MemoryMultiplexerStore>();
    assert_type::<eventuary::memory::subscriber_work::MemorySubscriberWorkStore>();
    assert_type::<eventuary::memory::watermark::MemoryWatermarkStore>();

    assert_type::<eventuary::sqlite::reader::SqliteReader>();
    assert_type::<eventuary::sqlite::reader::SqliteSubscription>();
    assert_type::<eventuary::sqlite::reader::SqliteCursor>();
    assert_type::<eventuary::sqlite::reader::SqliteCursorAcker>();
    assert_type::<eventuary::sqlite::reader::SqliteReaderConfig>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedReader>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedSubscription>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedReaderConfig>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedCursor>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedStream>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedAcker>();
    assert_type::<eventuary::sqlite::reader::SqliteCoordinatedStreamAcker>();
    assert_type::<eventuary::sqlite::reader::SqlitePartitionedCursor>();
    assert_type::<eventuary::sqlite::writer::SqliteWriter>();
    assert_type::<eventuary::sqlite::writer::SqliteWriterConfig>();
    assert_type::<eventuary::sqlite::writer::SqlitePartitioningConfig>();
    assert_type::<eventuary::sqlite::checkpoint::SqliteCheckpointStore<eventuary::sqlite::reader::SqliteCursor>>();
    assert_type::<eventuary::sqlite::checkpoint::SqliteCheckpointStoreConfig>();
    assert_type::<eventuary::sqlite::coordinator::SqlitePartitionCoordinator>();
    assert_type::<eventuary::sqlite::coordinator::SqlitePartitionCoordinatorConfig>();
    assert_type::<eventuary::sqlite::database::SqliteDatabase>();
    assert_type::<eventuary::sqlite::database::SqliteDatabaseConfig>();
    assert_type::<eventuary::sqlite::relation::SqliteRelationName>();
    assert_type::<eventuary::sqlite::buffer::SqliteBufferStore<eventuary::sqlite::reader::SqliteCursor>>();
    assert_type::<eventuary::sqlite::buffer::SqliteBufferStoreConfig>();
    assert_type::<eventuary::sqlite::buffer::SqliteBufferStoreId>();
    assert_type::<eventuary::sqlite::dedupe::SqliteDedupeStore>();
    assert_type::<eventuary::sqlite::dedupe::SqliteDedupeStoreConfig>();
    assert_type::<eventuary::sqlite::multiplexer::SqliteMultiplexerStore>();
    assert_type::<eventuary::sqlite::multiplexer::SqliteMultiplexerStoreConfig>();
    assert_type::<eventuary::sqlite::watermark::SqliteWatermarkStore>();
    assert_type::<eventuary::sqlite::watermark::SqliteWatermarkStoreConfig>();
    assert_type::<eventuary::sqlite::partitioning::SqlitePartitionBackfill>();
    assert_type::<eventuary::sqlite::partitioning::SqlitePartitionBackfillConfig>();
    assert_type::<eventuary::sqlite::partitioning::BackfillReport>();

    assert_type::<eventuary::postgres::reader::PgReader>();
    assert_type::<eventuary::postgres::reader::PgSubscription>();
    assert_type::<eventuary::postgres::reader::PgCursor>();
    assert_type::<eventuary::postgres::reader::PgCursorAcker>();
    assert_type::<eventuary::postgres::reader::PgReaderConfig>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedReader>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedSubscription>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedReaderConfig>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedCursor>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedStream>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedAcker>();
    assert_type::<eventuary::postgres::reader::PgCoordinatedStreamAcker>();
    assert_type::<eventuary::postgres::reader::PgPartitionedCursor>();
    assert_type::<eventuary::postgres::writer::PgWriter>();
    assert_type::<eventuary::postgres::writer::PgWriterConfig>();
    assert_type::<eventuary::postgres::writer::PgPartitioningConfig>();
    assert_type::<eventuary::postgres::checkpoint::PgCheckpointStore<eventuary::postgres::reader::PgCursor>>();
    assert_type::<eventuary::postgres::checkpoint::PgCheckpointStoreConfig>();
    assert_type::<eventuary::postgres::coordinator::PgPartitionCoordinator>();
    assert_type::<eventuary::postgres::coordinator::PgPartitionCoordinatorConfig>();
    assert_type::<eventuary::postgres::database::PgDatabase>();
    assert_type::<eventuary::postgres::database::PgDatabaseConfig>();
    assert_type::<eventuary::postgres::database::PgConnectOptions>();
    assert_type::<eventuary::postgres::relation::PgRelationName>();
    assert_type::<eventuary::postgres::buffer::PgBufferStore<eventuary::postgres::reader::PgCursor>>();
    assert_type::<eventuary::postgres::buffer::PgBufferStoreConfig>();
    assert_type::<eventuary::postgres::buffer::PgBufferStoreId>();
    assert_type::<eventuary::postgres::claim_buffer::PgClaimedBufferStore>();
    assert_type::<eventuary::postgres::claim_buffer::PgClaimedBufferStoreConfig>();
    assert_type::<eventuary::postgres::dedupe::PgDedupeStore>();
    assert_type::<eventuary::postgres::dedupe::PgDedupeStoreConfig>();
    assert_type::<eventuary::postgres::multiplexer::PgMultiplexerStore>();
    assert_type::<eventuary::postgres::multiplexer::PgMultiplexerStoreConfig>();
    assert_type::<eventuary::postgres::watermark::PgWatermarkStore>();
    assert_type::<eventuary::postgres::watermark::PgWatermarkStoreConfig>();
    assert_type::<eventuary::postgres::partitioning::PgPartitionBackfill>();
    assert_type::<eventuary::postgres::partitioning::PgPartitionBackfillConfig>();
    assert_type::<eventuary::postgres::partitioning::BackfillReport>();

    assert_type::<eventuary::sqs::reader::SqsReader>();
    assert_type::<eventuary::sqs::reader::SqsSubscription>();
    assert_type::<eventuary::sqs::reader::SqsReaderConfig>();
    assert_type::<eventuary::sqs::writer::SqsWriter>();
    assert_type::<eventuary::sqs::flusher::SqsFlusher>();

    assert_type::<eventuary::kafka::reader::KafkaReader>();
    assert_type::<eventuary::kafka::reader::KafkaSubscription>();
    assert_type::<eventuary::kafka::reader::KafkaReaderConfig>();
    assert_type::<eventuary::kafka::reader::KafkaCursor>();
    assert_type::<eventuary::kafka::writer::KafkaWriter>();
    assert_type::<eventuary::kafka::flusher::KafkaFlusher>();
    assert_type::<eventuary::kafka::flusher::KafkaOffsetToken>();
}
