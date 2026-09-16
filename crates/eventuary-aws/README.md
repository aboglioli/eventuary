# eventuary-aws

AWS event backends for [eventuary](https://crates.io/crates/eventuary). Supersedes `eventuary-sqs`.

Because this crate covers more than one AWS service, its modules are nested by **service first, role second** — `eventuary_aws::sqs::reader::SqsReader` — rather than exposing role modules at the crate root like the single-service backend crates do.

## SQS

`sqs::writer::SqsWriter` publishes serialized events as queue messages. `sqs::reader::SqsReader` long-polls a queue, deserializes messages, and emits a `Message<BatchedAcker<String>, NoCursor>` whose token is the SQS receipt handle. Acks are batched into `DeleteMessageBatch` calls; nacks become `ChangeMessageVisibilityBatch` with a zero visibility timeout. SQS only supports `StartFrom::Latest`; `Earliest`, `Timestamp`, `limit`, and `consumer_group_id` are rejected at config construction with `Error::Config`. Poison records (missing body, malformed JSON, undecodable event) are acked and skipped.

## Testing

Integration tests use [`testcontainers`](https://crates.io/crates/testcontainers) to spawn LocalStack at `localstack/localstack:3.8.1`; on rootless podman set `DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock` and `TESTCONTAINERS_RYUK_DISABLED=true` before invoking `cargo test`.
