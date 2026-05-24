//! Eventuary: typed event model, async IO traits, and feature-gated backends.
//!
//! `eventuary` is an umbrella crate that re-exports the
//! [`eventuary-core`](https://crates.io/crates/eventuary-core) types and IO
//! traits at its root, and exposes each backend behind a Cargo feature flag:
//!
//! | Feature | Module | Backend crate |
//! |---------|--------|---------------|
//! | `memory` | [`memory`] | [`eventuary-memory`](https://crates.io/crates/eventuary-memory) |
//! | `sqlite` | [`sqlite`] | [`eventuary-sqlite`](https://crates.io/crates/eventuary-sqlite) |
//! | `postgres` | [`postgres`] | [`eventuary-postgres`](https://crates.io/crates/eventuary-postgres) |
//! | `sqs` | [`sqs`] | [`eventuary-sqs`](https://crates.io/crates/eventuary-sqs) |
//! | `kafka` | [`kafka`] | [`eventuary-kafka`](https://crates.io/crates/eventuary-kafka) |
//!
//! No backend is enabled by default. Pick the ones you need:
//!
//! ```toml
//! [dependencies]
//! eventuary = { version = "0.1.0-alpha.1", features = ["postgres"] }
//! ```
//!
//! Backend-authoring crates may prefer to depend directly on `eventuary-core`.
//!
//! # Example
//!
//! ```
//! use eventuary::{Event, Payload};
//!
//! let event = Event::builder(
//!     "acme",
//!     "/billing",
//!     "invoice.created",
//!     Payload::from_json(&serde_json::json!({"amount": 100})).unwrap(),
//! ).unwrap()
//! .key("invoice-123").unwrap()
//! .build().unwrap();
//! assert_eq!(event.topic().as_str(), "invoice.created");
//! ```
//!
//! # Typed Payloads
//!
//! `Event<P = Payload>` is generic over the payload type. In-memory and
//! in-process pipelines can carry a typed `P` end-to-end without
//! serialization; durable backends (SQL, SQS, Kafka) stay bound to
//! `Payload`. Bridge with `io::reader::DecodeReader` and
//! `io::writer::EncodeWriter` when crossing the durable boundary.
//!
//! ```
//! use eventuary::Event;
//!
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! struct OrderPlaced { order_id: String, total: u64 }
//!
//! let event: Event<OrderPlaced> = Event::create(
//!     "acme",
//!     "/orders",
//!     "order.placed",
//!     OrderPlaced { order_id: "o-1".to_owned(), total: 42 },
//! ).unwrap();
//! assert_eq!(event.payload().order_id, "o-1");
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use eventuary_core::*;

#[cfg(feature = "memory")]
#[cfg_attr(docsrs, doc(cfg(feature = "memory")))]
pub use eventuary_memory as memory;

#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub use eventuary_sqlite as sqlite;

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub use eventuary_postgres as postgres;

#[cfg(feature = "sqs")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqs")))]
pub use eventuary_sqs as sqs;

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub use eventuary_kafka as kafka;
