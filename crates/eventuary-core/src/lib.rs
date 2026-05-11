//! Core event model and async IO traits for the eventuary toolkit.
//!
//! This crate provides the typed event model ([`Event`], [`Payload`], [`Topic`],
//! [`Namespace`], [`OrganizationId`]), serialization helpers ([`SerializedEvent`]),
//! and async IO traits for backends ([`Reader`], [`Writer`], [`Handler`], [`Acker`]).
//!
//! Most consumers should depend on the [`eventuary`] umbrella crate and pick
//! backends via Cargo features, instead of using `eventuary-core` directly.
//! Use this crate when you are implementing a new backend or want only the
//! core types without any backend.
//!
//! # Example
//!
//! ```
//! use eventuary_core::{Event, Payload};
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
//! [`eventuary`]: https://crates.io/crates/eventuary
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
mod serialization;
mod snapshot;
mod start_from;
mod subscription;
mod topic;

pub use collector::EventCollector;
pub use consumer_group_id::ConsumerGroupId;
pub use cursor::{EventCursor, EventSequence};
pub use error::{Error, Result};
pub use event::{Event, EventId, RestoreEvent};
pub use event_key::EventKey;
pub use io::{
    Acker, AckerExt, ArcAcker, ArcFilter, ArcHandler, ArcReader, ArcWriter, BackgroundConsumer,
    BoxAcker, BoxFilter, BoxFuture, BoxHandler, BoxReader, BoxStream, BoxWriter, ConsumerHandle,
    DeadLetterWriter, DefaultRetryPolicy, DynAcker, DynHandler, DynReader, DynWriter, Filter,
    FilterExt, FilteredHandler, Handler, HandlerExt, Message, Reader, ReaderExt, RetryAction,
    RetryConfig, RetryHandler, RetryPolicy, Writer, WriterExt, backoff_delay,
};
pub use metadata::{CAUSATION_ID, CORRELATION_ID, Metadata};
pub use namespace::Namespace;
pub use organization::OrganizationId;
pub use partition::PartitionKey;
pub use payload::{ContentType, Payload};
pub use serialization::SerializedEvent;
pub use snapshot::{Snapshot, SnapshotEventId};
pub use start_from::StartFrom;
pub use subscription::EventSubscription;
pub use topic::Topic;
