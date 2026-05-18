# eventuary-conformance

Internal workspace scaffold for shared backend conformance checks. This crate is not published to crates.io.

It currently exposes [`Capabilities`] so backend tests can describe supported cursor-reader behavior while the reusable case suite is rebuilt. Backend-specific integration and composition tests in `crates/eventuary-<backend>/tests/` are the source of truth for release validation.
