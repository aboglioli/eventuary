# eventuary-postgres

PostgreSQL event backend for [eventuary](https://crates.io/crates/eventuary). Provides a durable, multi-node event log with consumer-group checkpointing on top of `sqlx::PgPool`. Schema is created automatically by `PgDatabase::connect`. Integration tests use [`testcontainers`](https://crates.io/crates/testcontainers) to spawn a real Postgres container; when running on rootless podman set `DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock` and `TESTCONTAINERS_RYUK_DISABLED=true` before invoking `cargo test`.
