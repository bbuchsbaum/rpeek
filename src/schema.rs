//! JSON schema support for the request and response contract.
//!
//! The checked-in schema files are embedded at compile time so the CLI can print them
//! directly via `rpeek schema request` and `rpeek schema response`.
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
