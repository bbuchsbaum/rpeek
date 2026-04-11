//! Rust-facing documentation for `rpeek`.
//!
//! `rpeek` is a CLI-first tool for inspecting installed R packages, but it also exposes a
//! small internal Rust surface that is useful to understand when working on the daemon,
//! protocol, or response pipeline.
//!
//! The generated docs are intended for maintainers and contributors:
//!
//! - [`protocol`] defines the typed JSON request contract used by the daemon and R helper.
//! - [`response`] handles response post-processing and exit-code logic.
//! - [`schema`] exposes the embedded JSON schemas that describe the wire contract.
//!
//! # Common CLI Flows
//!
//! ```text
//! rpeek summary stats lm
//! rpeek doc stats lm
//! rpeek search-all lm
//! rpeek resolve lm
//! rpeek daemon status
//! rpeek --no-daemon sig stats lm
//! ```
//!
//! # Architecture
//!
//! 1. The Rust CLI parses a command into a [`protocol::Request`].
//! 2. The client either talks to a long-lived daemon or runs a one-shot helper.
//! 3. The daemon forwards JSON Lines requests to the embedded R helper script.
//! 4. The Rust side applies [`response::ResponseOptions`] before printing JSON.
//!
//! To browse these docs locally:
//!
//! ```text
//! cargo doc --no-deps --open
//! ```

pub mod protocol;
pub mod response;
pub mod schema;

pub use protocol::Request;
pub use response::{
    ResponseOptions, apply_response_options, response_exit_code, response_is_success,
    response_reports_success,
};
pub use schema::{SchemaKind, schema_response};
