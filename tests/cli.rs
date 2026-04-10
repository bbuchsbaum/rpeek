use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

struct DaemonGuard {
    socket: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        shutdown_daemon(&self.socket);
    }
}

fn shutdown_daemon(socket: &Path) {
    let _ = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .arg("shutdown")
        .env("RPEEK_SOCKET", socket)
        .output();
}

fn run(args: &[&str]) -> (i32, String) {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-test.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    run_with_socket(&socket, args)
}

fn run_with_socket(socket: &Path, args: &[&str]) -> (i32, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .args(args)
        .env("RPEEK_SOCKET", socket)
        .output()
        .expect("failed to run rpeek");

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf8");
    (output.status.code().unwrap_or(-1), stdout)
}

#[test]
fn pkg_returns_metadata() {
    let (code, stdout) = run(&["pkg", "utils"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["ok"], true);
    assert_eq!(value["payload"]["package"], "utils");
    assert!(value["payload"]["version"].is_string());
}

#[test]
fn sig_returns_formals() {
    let (code, stdout) = run(&["sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let signature = value["payload"]["signature"]
        .as_str()
        .expect("missing signature");
    assert!(signature.contains("function (formula, data, subset"));
}

#[test]
fn source_returns_kind_and_text() {
    let (code, stdout) = run(&["source", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["kind"], "deparsed");
    let text = value["payload"]["text"].as_str().expect("missing text");
    assert!(text.contains("ret.x <- x"));
}

#[test]
fn doc_returns_usage() {
    let (code, stdout) = run(&["doc", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["topic"], "lm");
    let aliases = value["payload"]["aliases"]
        .as_array()
        .expect("aliases should be an array");
    assert!(aliases.iter().any(|alias| alias == "lm"));
    let usage = value["payload"]["usage"].as_str().expect("missing usage");
    assert!(usage.contains("lm(formula, data"));
    assert!(usage.contains("print(x, digits"));
    assert!(!usage.contains("printlm("));
}

#[test]
fn cache_stats_and_clear_work() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-cache.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["cache", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(stats["payload"]["entries"], 0);
    assert_eq!(stats["payload"]["hits"], 0);
    assert_eq!(stats["payload"]["misses"], 0);

    let (code, stdout) = run_with_socket(&socket, &["sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["name"], "lm");

    let (code, stdout) = run_with_socket(&socket, &["cache", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(stats["payload"]["entries"], 1);
    assert_eq!(stats["payload"]["packages"], 1);
    assert_eq!(stats["payload"]["hits"], 0);
    assert_eq!(stats["payload"]["misses"], 1);

    let (code, stdout) = run_with_socket(&socket, &["sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let (code, stdout) = run_with_socket(&socket, &["cache", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(stats["payload"]["entries"], 1);
    assert_eq!(stats["payload"]["hits"], 1);
    assert_eq!(stats["payload"]["misses"], 1);

    let (code, stdout) = run_with_socket(&socket, &["cache", "clear"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let cleared: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(cleared["payload"]["cleared_entries"], 1);
    assert_eq!(cleared["payload"]["cleared_packages"], 1);

    let (code, stdout) = run_with_socket(&socket, &["cache", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(stats["payload"]["entries"], 0);
    assert_eq!(stats["payload"]["packages"], 0);
}

#[test]
fn search_returns_matches() {
    let (code, stdout) = run(&["search", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "search");
    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
}

#[test]
fn search_kind_and_limit_work() {
    let (code, stdout) = run(&["search", "--kind", "topic", "--limit", "3", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
    assert!(matches.len() <= 3);
    assert!(matches.iter().all(|entry| entry["kind"] == "topic"));
}

#[test]
fn search_all_handles_quoted_query() {
    let (code, stdout) = run(&["search-all", "--limit", "5", "\"lm\""]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "search_all");
    assert_eq!(value["payload"]["query"], "\"lm\"");
    assert!(value["payload"]["matches"].is_array());
}

#[test]
fn search_all_finds_stats_lm() {
    let (code, stdout) = run(&["search-all", "--kind", "object", "--limit", "10", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "search_all");
    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
    assert!(matches.len() <= 10);
    assert!(matches.iter().all(|entry| entry["kind"] == "object"));
    assert!(
        matches
            .iter()
            .any(|entry| entry["package"] == "stats" && entry["name"] == "lm")
    );
}

#[test]
fn summary_returns_combined_payload() {
    let (code, stdout) = run(&["summary", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "summary");
    assert_eq!(value["payload"]["object"]["name"], "lm");
    assert_eq!(value["payload"]["source"]["kind"], "deparsed");
    assert!(value["payload"]["doc"]["title"].as_str().is_some());
}

#[test]
fn missing_object_returns_suggestions() {
    let (code, stdout) = run(&["sig", "stats", "lmx"]);
    assert_eq!(code, 2, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["ok"], false);
    assert_eq!(value["error"]["code"], "object_not_found");
    assert!(value["error"]["suggestions"].is_array());
    assert!(value["error"]["hint"].as_str().is_some());
}

#[test]
fn missing_package_returns_structured_error() {
    let (code, stdout) = run(&["sig", "definitely_missing_rpeek_package", "lm"]);
    assert_eq!(code, 2, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["ok"], false);
    assert_eq!(value["error"]["code"], "package_not_found");
    assert!(value["error"]["hint"].as_str().is_some());
}

#[test]
fn batch_returns_nonzero_when_any_item_fails() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-batch-error.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    let batch_file = tempdir.path().join("requests.jsonl");
    fs::write(
        &batch_file,
        concat!(
            r#"{"action":"summary","package":"stats","name":"lm"}"#,
            "\n",
            r#"{"action":"sig","package":"stats","name":"lmx"}"#,
            "\n"
        ),
    )
    .expect("failed to write batch file");

    let output = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .args(["batch", "--file", batch_file.to_str().expect("utf8 path")])
        .env("RPEEK_SOCKET", &socket)
        .output()
        .expect("failed to run rpeek batch");

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf8");
    assert_eq!(output.status.code().unwrap_or(-1), 2, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let responses = value["payload"]["responses"]
        .as_array()
        .expect("missing responses");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["ok"], true);
    assert_eq!(responses[1]["ok"], false);
    assert_eq!(responses[1]["error"]["code"], "object_not_found");
}

#[test]
fn batch_returns_multiple_responses() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-batch.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    let batch_file = tempdir.path().join("requests.jsonl");
    fs::write(
        &batch_file,
        concat!(
            r#"{"action":"summary","package":"stats","name":"lm"}"#,
            "\n",
            r#"{"action":"sig","package":"stats","name":"lm"}"#,
            "\n"
        ),
    )
    .expect("failed to write batch file");

    let output = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .args(["batch", "--file", batch_file.to_str().expect("utf8 path")])
        .env("RPEEK_SOCKET", &socket)
        .output()
        .expect("failed to run rpeek batch");

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf8");
    assert_eq!(output.status.code().unwrap_or(-1), 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let responses = value["payload"]["responses"]
        .as_array()
        .expect("missing responses");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["command"], "summary");
    assert_eq!(responses[1]["command"], "sig");
}
