//! Eventuary core: event model and async IO traits.
//!
//! This crate provides the typed event model ([`Event`], [`Payload`], [`Topic`],
//! [`Namespace`], [`OrganizationId`]), serialization helpers ([`SerializedEvent`]),
//! and async IO traits for backends ([`Reader`], [`Writer`], [`Handler`], [`Acker`]).
//!
//! Backends (memory, SQLite, Postgres, SQS, Kafka) live in sibling crates and
//! depend on this one.
//!
//! # Example
//!
//! ```
//! use eventuary::{Event, Payload};
//!
//! let event = Event::create(
//!     "acme",
//!     "/billing",
//!     "invoice.created",
//!     "invoice-123",
//!     Payload::from_json(&serde_json::json!({"amount": 100})).unwrap(),
//! ).unwrap();
//! assert_eq!(event.topic().as_str(), "invoice.created");
//! ```
//!
//! See the [project README](https://github.com/aboglioli/eventuary) for the full
//! tour and backend usage examples.
#![cfg_attr(docsrs, feature(doc_cfg))]

mod collector;
mod consumer_group_id;
mod cursor;
mod error;
mod event;
mod event_key;
pub mod io;
mod metadata;
mod namespace;
mod organization;
mod partition;
mod payload;
mod selector;
mod serialization;
mod snapshot;
mod start_from;
mod topic;

pub use collector::EventCollector;
pub use consumer_group_id::ConsumerGroupId;
pub use cursor::{EventCursor, EventSequence};
pub use error::{Error, Result};
pub use event::{Event, EventId, RestoreEvent};
pub use event_key::EventKey;
pub use io::{
    Acker, AckerExt, ArcAcker, ArcFilter, ArcHandler, ArcReader, ArcWriter, BackgroundConsumer,
    BoxAcker, BoxFilter, BoxHandler, BoxReader, BoxStream, BoxWriter, ConsumerHandle,
    DeadLetterWriter, DefaultRetryPolicy, DynAcker, DynHandler, DynReader, DynWriter, Filter,
    FilterExt, FilteredHandler, Handler, HandlerExt, Message, Reader, ReaderExt, RetryAction,
    RetryConfig, RetryHandler, RetryPolicy, Writer, WriterExt, backoff_delay,
};
pub use metadata::{CAUSATION_ID, CORRELATION_ID, Metadata};
pub use namespace::Namespace;
pub use organization::OrganizationId;
pub use partition::PartitionKey;
pub use payload::{ContentType, Payload};
pub use selector::EventSelector;
pub use serialization::SerializedEvent;
pub use snapshot::{Snapshot, SnapshotEventId};
pub use start_from::StartFrom;
pub use topic::Topic;
