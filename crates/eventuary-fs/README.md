# eventuary-fs

Filesystem event backend for [eventuary](https://crates.io/crates/eventuary). Provides a durable, partitioned, replayable event log on ordinary files, with consumer-group coordination — and no database, broker, or C dependency.

The log is organised the way a log-structured broker organises one: a directory per partition, size-rolled segments named by base offset, and sparse offset and time indexes so a read seeks near its target instead of scanning. Records are JSON lines carrying a flat `offset` field, so the log stays readable with `cat`, `grep` and `jq`.

Offsets are dense and monotonic within a partition, which makes the cursor a plain integer and lets `FsReader` compose with `PartitionedReader::source_from_cursor`, `CheckpointReader` and `FsCoordinatedReader` exactly as the SQL backends do.

**A log has one producer process.** `FsWriter::open` takes an exclusive advisory lock on every partition, which is what keeps offsets dense; a second process opening the same root fails instead of interleaving. Consumers are unrestricted — any number of processes share a log through `FsPartitionCoordinator`, which assigns partitions per consumer group under fenced leases. Use `FsWriter::open_partitions_subset` to split production across processes, remembering that a partition is chosen by hashing the event key.

Appends fsync on the `SyncPolicy` schedule, which defaults to once per megabyte; choose `SyncPolicy::Always` when no acknowledged event may be lost. Checkpoint and coordinator state is written through a temp file, rename, and directory fsync, so a surviving file is never a partial one. A record that no longer decodes into an `Event` ends that partition's stream, naming its partition and offset, and keeps ending it until someone intervenes — skipping it would lose an event the log still holds. Segments are JSON lines, so the repair is to correct or delete that line. `FsBufferStore` numbers entries from an in-process counter, so one buffer directory belongs to one process.

Retention is caller-driven: `FsWriter::enforce_retention` deletes segments a `RetentionPolicy` has aged or sized out, and nothing calls it for you. A consumer whose checkpoint falls behind the retained range gets an error rather than a silent skip.

Tests need no containers: `cargo test -p eventuary-fs`.
