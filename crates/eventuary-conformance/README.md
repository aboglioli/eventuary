# eventuary-conformance

Shared backend conformance test suite for eventuary. Not published to crates.io.

Each backend's integration tests implement the [`Backend`] trait, declare their
[`Capabilities`], and call `run_all`. Capability flags gate cases that the
backend cannot honor (e.g. SQS does not support replay, so
`start_from_earliest` is skipped).

TODO: SQS and Kafka adapters can be added later. The suite is currently wired
into memory, SQLite, and Postgres backends.
