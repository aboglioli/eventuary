# eventuary-fs

Filesystem event backend for [eventuary](https://crates.io/crates/eventuary). Provides a durable, partitioned, replayable event log on ordinary files, with consumer-group coordination — and no database, broker, or C dependency.

The log is organised the way a log-structured broker organises one: a directory per partition, size-rolled segments named by base offset, and sparse offset and time indexes so a read seeks near its target instead of scanning. Records are JSON lines carrying a flat `offset` field, so the log stays readable with `cat`, `grep` and `jq`.

Offsets are dense and monotonic within a partition, which makes the cursor a plain integer and lets `FsReader` compose with `PartitionedReader::source_from_cursor`, `CheckpointReader` and `FsCoordinatedReader` exactly as the SQL backends do. A partition has at most one writer at a time, enforced by an advisory file lock released when the process exits.

Tests need no containers: `cargo test -p eventuary-fs`.
