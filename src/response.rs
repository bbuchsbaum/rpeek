//! Response shaping utilities shared by the CLI and library consumers.
//!
//! This module is intentionally protocol-agnostic. It operates on parsed JSON values,
//! letting callers enforce the same output policy as the CLI: trim oversized string
//! fields, remove examples, and derive a process-style success code from a response.
//!
//! ```
//! use rpeek::{ResponseOptions, apply_response_options, response_exit_code};
//! use serde_json::json;
//!
//! let mut value = json!({
//!     "ok": true,
//!     "payload": {
//!         "examples": "example code",
//!         "text": "abcdefghijklmnopqrstuvwxyz"
//!     }
//! });
//!
//! apply_response_options(
//!     &mut value,
//!     &ResponseOptions {
//!         max_bytes: Some(6),
//!         no_examples: true,
//!     },
//! );
//!
//! assert_eq!(response_exit_code(&value), 0);
//! assert!(value["payload"].get("examples").is_none());
//! assert!(value["payload"]["text"].as_str().unwrap().contains("[truncated"));
//! ```
//!
use serde_json::Value;

/// Output-shaping options applied after a JSON response has been parsed.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResponseOptions {
    /// Maximum number of bytes allowed in any single string field.
    pub max_bytes: Option<usize>,
    /// Remove `examples` fields from nested payloads.
    pub no_examples: bool,
}

/// Determine the CLI exit code from a top-level JSON response.
pub fn response_exit_code(value: &Value) -> i32 {
    if !response_reports_success(value) {
        return 2;
    }

    if let Some(responses) = value
        .get("payload")
        .and_then(|payload| payload.get("responses"))
        .and_then(Value::as_array)
        && responses
            .iter()
            .any(|response| !response_reports_success(response))
    {
        return 2;
    }

    0
}

/// Return whether a parsed response reports success.
pub fn response_reports_success(value: &Value) -> bool {
    value.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

/// Return whether a raw JSON response string reports success.
pub fn response_is_success(response: &str) -> bool {
    serde_json::from_str::<Value>(response)
        .ok()
        .and_then(|value| value.get("ok").and_then(Value::as_bool))
        .unwrap_or(false)
}

/// Apply post-processing options to a parsed JSON response.
pub fn apply_response_options(value: &mut Value, options: &ResponseOptions) {
    if options.no_examples {
        remove_key_recursive(value, "examples");
    }
    if let Some(max_bytes) = options.max_bytes {
        trim_strings_recursive(value, max_bytes);
    }
}

fn remove_key_recursive(value: &mut Value, key: &str) {
    match value {
        Value::Object(map) => {
            map.remove(key);
            for child in map.values_mut() {
                remove_key_recursive(child, key);
            }
        }
        Value::Array(items) => {
            for child in items {
                remove_key_recursive(child, key);
            }
        }
        _ => {}
    }
}

fn trim_strings_recursive(value: &mut Value, max_bytes: usize) {
    match value {
        Value::String(text) if text.len() > max_bytes => {
            let mut end = max_bytes;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let omitted = text.len() - end;
            text.truncate(end);
            text.push_str(&format!("\n...[truncated {omitted} bytes]"));
        }
        Value::Array(items) => {
            for child in items {
                trim_strings_recursive(child, max_bytes);
            }
        }
        Value::Object(map) => {
            for child in map.values_mut() {
                trim_strings_recursive(child, max_bytes);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn batch_exit_code_fails_when_any_item_fails() {
        let value = json!({
            "ok": true,
            "payload": {
                "responses": [
                    { "ok": true },
                    { "ok": false }
                ]
            }
        });

        assert_eq!(response_exit_code(&value), 2);
    }
}
