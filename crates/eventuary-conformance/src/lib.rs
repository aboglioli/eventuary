//! Conformance test suite for eventuary backends.
//!
//! Capability flags ([`Capabilities`]) describe which cursor-reader
//! composition features a backend supports. The shared case suite is
//! deferred to a future task — backends currently cover the new API via
//! their own integration/composition tests in
//! `crates/eventuary-<backend>/tests/`.

mod capabilities;

pub use capabilities::Capabilities;
