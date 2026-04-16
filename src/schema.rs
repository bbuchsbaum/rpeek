//! Embedded JSON schemas for the `rpeek` request and response contract.
//!
//! The schema files in `schemas/` are compiled into the binary so callers can inspect
//! the wire contract without locating files on disk. This is the same data returned by
//! the CLI's `schema` command.
//!
//! ```
//! use rpeek::{SchemaKind, schema_response};
//!
//! let request_schema = schema_response(SchemaKind::Request);
//!
//! assert_eq!(request_schema["ok"].as_bool(), Some(true));
//! assert_eq!(request_schema["command"].as_str(), Some("schema"));
//! assert!(request_schema["payload"].is_object());
//! ```
//!
use clap::ValueEnum;
use serde_json::{Value, json};

const REQUEST_SCHEMA: &str = include_str!("../schemas/request.schema.json");
const RESPONSE_SCHEMA: &str = include_str!("../schemas/response.schema.json");

/// Select which JSON schema to print.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SchemaKind {
    /// The helper request schema.
    Request,
    /// The CLI/helper response schema.
    Response,
}

/// Return a JSON response containing one of the embedded schema documents.
pub fn schema_response(kind: SchemaKind) -> Value {
    let payload: Value = match kind {
        SchemaKind::Request => serde_json::from_str(REQUEST_SCHEMA).expect("valid request schema"),
        SchemaKind::Response => {
            serde_json::from_str(RESPONSE_SCHEMA).expect("valid response schema")
        }
    };

    json!({
        "schema_version": 1,
        "ok": true,
        "command": "schema",
        "payload": payload
    })
}
