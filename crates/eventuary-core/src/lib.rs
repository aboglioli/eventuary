//! Core event model and async IO traits for the eventuary toolkit.
//!
//! This crate provides the typed event model ([`Event`], [`Payload`], [`Topic`],
//! [`Namespace`], [`OrganizationId`]), serialization helpers ([`SerializedEvent`]),
//! and async IO traits for backends ([`io::Reader`], [`io::Writer`], [`io::Handler`], [`io::Acker`]).
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
//! [`eventuary`]: https://crates.io/crates/eventuary
#![cfg_attr(docsrs, feature(doc_cfg))]

mod collector;
mod error;
mod event;
mod event_key;
pub mod io;
mod metadata;
mod namespace;
mod namespace_pattern;
mod organization;
mod payload;
mod serialization;
mod snapshot;
mod topic;
mod topic_pattern;

pub use collector::EventCollector;
pub use error::{Error, Result};
pub use event::{Event, EventId};
pub use event_key::{EventKey, Partition};
pub use metadata::Metadata;
pub use namespace::Namespace;
pub use namespace_pattern::NamespacePattern;
pub use organization::OrganizationId;

pub use io::start_from::{StartFrom, StartableSubscription};
pub use payload::{ContentType, Payload};
pub use serialization::{SerializedEvent, SerializedPayload};
pub use snapshot::{Snapshot, SnapshotEventId};
pub use topic::Topic;
pub use topic_pattern::TopicPattern;
