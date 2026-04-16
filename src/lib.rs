//! Rust API for `rpeek`.
//!
//! `rpeek` is primarily a CLI for inspecting installed R packages, but the crate also
//! exposes a compact Rust API for callers that need to:
//!
//! - construct typed requests for the CLI/daemon/helper protocol
//! - apply the same response-shaping rules used by the CLI
//! - retrieve the embedded request and response schemas
//!
//! The public API is intentionally small. It is best thought of as a library for
//! wrappers, tests, and tooling that sit close to the `rpeek` executable rather than a
//! large end-user SDK.
//!
//! # Getting Started
//!
//! Most Rust consumers start with [`Request`], then optionally use [`ResponseOptions`]
//! to shape parsed JSON responses.
//!
//! ```
//! use rpeek::{Request, ResponseOptions, apply_response_options};
//! use serde_json::json;
//!
//! let request = Request::Summary {
//!     package: "stats".to_string(),
//!     name: "lm".to_string(),
//! };
//!
//! assert_eq!(request.action(), "summary");
//! assert_eq!(request.package(), Some("stats"));
//!
//! let mut response = json!({
//!     "ok": true,
//!     "payload": {
//!         "examples": "lm(y ~ x)",
//!         "text": "abcdefghijklmnopqrstuvwxyz"
//!     }
//! });
//!
//! apply_response_options(
//!     &mut response,
//!     &ResponseOptions {
//!         max_bytes: Some(8),
//!         no_examples: true,
//!     },
//! );
//!
//! assert!(response["payload"].get("examples").is_none());
//! assert!(response["payload"]["text"]
//!     .as_str()
//!     .unwrap()
//!     .contains("[truncated"));
//! ```
//!
//! # Rust API
//!
//! [`Request`]
//! : The typed wire-format enum used by the CLI, daemon, and R helper.
//!
//! [`ResponseOptions`], [`apply_response_options`], [`response_exit_code`]
//! : Mirror the output-shaping and success/exit-code logic used by the CLI.
//!
//! [`SchemaKind`], [`schema_response`]
//! : Return the checked-in JSON schema documents that define the request/response
//! contract.
//!
//! # Module Guide
//!
//! - [`protocol`]: request types and request metadata helpers such as
//!   [`Request::action`] and [`Request::package`]
//! - [`response`]: response trimming, example removal, and success/exit-code helpers
//! - [`schema`]: embedded request and response schemas
//!
//! # Notes
//!
//! This crate does not currently expose a high-level Rust client for spawning the
//! daemon, talking over the socket, or executing requests end-to-end. The public Rust
//! surface is the data model and storage layer around those flows.
//!
//! To browse the generated crate docs locally:
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
