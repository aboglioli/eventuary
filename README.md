# Eventuary

Eventuary is a Rust event toolkit for logs, queues, streams, routing, replay, and acknowledgements across multiple backends.

It provides a small core event model and async IO traits, plus backend implementations for memory, SQLite, Postgres, SQS, and Kafka.

```toml
[dependencies]
eventuary = { version = "0.1.0-alpha.0", features = ["memory"] }
```

This project is currently alpha.
