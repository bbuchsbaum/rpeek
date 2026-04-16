use rusqlite::Connection;
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
    let index_path = socket
        .parent()
        .expect("socket should have parent")
        .join("rpeek-index.sqlite3");
    let output = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .args(args)
        .env("RPEEK_SOCKET", socket)
        .env("RPEEK_INDEX_PATH", &index_path)
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
fn map_returns_package_orientation_payload() {
    let (code, stdout) = run(&["map", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "map");
    assert_eq!(value["payload"]["package"], "stats");
    assert!(value["payload"]["counts"]["exports"].as_u64().unwrap_or(0) > 0);
    assert!(value["payload"]["dependencies"]["imports"].is_array());
    assert!(value["payload"]["entry_points"].is_array());
    assert!(value["payload"]["topic_samples"].is_array());
    assert!(value["payload"]["vignettes"].is_array());
    assert!(value["payload"]["file_samples"].is_array());
}

#[test]
fn methods_across_finds_generic_methods_for_indexed_packages() {
    let (code, stdout) = run(&[
        "methods-across",
        "plot",
        "--package",
        "stats",
        "--package",
        "graphics",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "methods_across");
    assert_eq!(value["payload"]["generic"], "plot");
    let methods = value["payload"]["methods"]
        .as_array()
        .expect("missing methods");
    assert!(!methods.is_empty());
    assert!(methods.iter().any(|entry| entry["package"] == "stats"));
}

#[test]
fn bridge_reports_direct_dependency_edges() {
    let (code, stdout) = run(&["bridge", "stats", "graphics"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "bridge");
    assert_eq!(value["payload"]["package"], "stats");
    assert_eq!(value["payload"]["other_package"], "graphics");
    let relations = value["payload"]["direct_relations"]["package_to_other"]
        .as_array()
        .expect("missing direct relations");
    assert!(relations.iter().any(|entry| entry == "imports"));
    assert_eq!(
        value["payload"]["direct_usage"]["package_to_other"]["namespace_import_all"],
        true
    );
    assert!(
        value["payload"]["direct_usage"]["package_to_other"]["file_mentions"]
            .as_array()
            .is_some()
    );
}

#[test]
fn xref_returns_symbol_payload() {
    let (code, stdout) = run(&["xref", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "xref");
    assert_eq!(value["payload"]["package"], "stats");
    assert_eq!(value["payload"]["symbol"], "lm");
    assert!(value["payload"]["local_mentions"]["files"].is_array());
}

#[test]
fn used_by_returns_symbol_payload() {
    let (code, stdout) = run(&["used-by", "graphics", "plot"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "used_by");
    assert_eq!(value["payload"]["package"], "graphics");
    assert_eq!(value["payload"]["symbol"], "plot");
    assert!(value["payload"]["callers"].is_array());
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
fn sigs_returns_exported_function_signatures() {
    let (code, stdout) = run(&["sigs", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "sigs");
    assert_eq!(value["payload"]["package"], "stats");
    assert_eq!(value["payload"]["all_objects"], false);
    let signatures = value["payload"]["signatures"]
        .as_array()
        .expect("missing signatures");
    assert!(!signatures.is_empty());
    assert!(
        signatures
            .iter()
            .all(|entry| entry["signature"].as_str().is_some())
    );
    assert!(
        signatures
            .iter()
            .any(|entry| entry["name"] == "lm" && entry["exported"] == true)
    );
}

#[test]
fn sigs_all_objects_includes_internal_functions() {
    let (code, stdout) = run(&["sigs", "--all-objects", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["all_objects"], true);
    let signatures = value["payload"]["signatures"]
        .as_array()
        .expect("missing signatures");
    assert!(
        signatures
            .iter()
            .any(|entry| entry["name"] == ".onLoad" && entry["exported"] == false)
    );
}

#[test]
fn vignettes_returns_installed_vignette_metadata() {
    let (code, stdout) = run(&["vignettes", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "vignettes");
    let vignettes = value["payload"]["vignettes"]
        .as_array()
        .expect("missing vignettes");
    assert!(!vignettes.is_empty());
    assert!(
        vignettes
            .iter()
            .any(|entry| entry["topic"] == "reshape" && entry["title"].as_str().is_some())
    );
}

#[test]
fn vignette_returns_text_for_known_vignette() {
    let (code, stdout) = run(&["vignette", "stats", "reshape"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "vignette");
    assert_eq!(value["payload"]["topic"], "reshape");
    let text = value["payload"]["text"].as_str().expect("missing text");
    assert!(text.contains("reshape"));
    assert!(value["payload"]["text_kind"].as_str().is_some());
}

#[test]
fn search_vignettes_finds_matching_metadata() {
    let (code, stdout) = run(&["search-vignettes", "utils", "Sweave"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "search_vignettes");
    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
    assert!(matches.iter().any(|entry| entry["topic"] == "Sweave"));
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
fn no_daemon_runs_single_request() {
    let (code, stdout) = run(&["--no-daemon", "sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "sig");
    assert_eq!(value["payload"]["name"], "lm");
}

#[test]
fn daemon_status_reports_running_daemon() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-status.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "daemon_status");
    assert_eq!(value["payload"]["status"], "running");
    assert!(value["payload"]["pid"].as_u64().is_some());
    assert!(value["payload"]["cache"]["max_entries"].as_u64().is_some());
}

#[test]
fn schema_command_returns_contract() {
    let (code, stdout) = run(&["schema", "request"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "schema");
    assert_eq!(value["payload"]["title"], "rpeek request");
}

#[test]
fn index_status_reports_schema_and_path() {
    let (code, stdout) = run(&["index", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "index_status");
    assert_eq!(value["payload"]["schema_version"], 6);
    assert!(value["payload"]["path"].as_str().is_some());
}

#[test]
fn snippet_commands_round_trip() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-snippet.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "add",
            "--title",
            "Read BIDS preproc scan",
            "--package",
            "bidser",
            "--package",
            "neuroim2",
            "--tag",
            "workflow",
            "--verb",
            "read",
            "--status",
            "verified",
            "--body",
            "Use bidser to locate scans, then read them with neuroim2.",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let added: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(added["command"], "snippet_add");
    let id = added["payload"]["id"].as_i64().expect("missing snippet id");

    let (code, stdout) = run_with_socket(&socket, &["snippet", "show", &id.to_string()]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let shown: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(shown["payload"]["title"], "Read BIDS preproc scan");
    assert_eq!(shown["payload"]["status"], "verified");
    assert_eq!(shown["payload"]["effective_status"], "verified");

    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "search",
            "bids workflow",
            "--package",
            "bidser",
            "--tag",
            "workflow",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
    assert_eq!(matches[0]["id"].as_i64(), Some(id));
    assert_eq!(matches[0]["effective_status"], "verified");
    assert_eq!(
        searched["payload"]["match_query"],
        "\"bids\" AND \"workflow\""
    );
    assert_eq!(searched["payload"]["raw_match"], false);

    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "list",
            "--package",
            "bidser",
            "--tag",
            "workflow",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let listed: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let snippets = listed["payload"]["snippets"]
        .as_array()
        .expect("missing snippets");
    assert!(
        snippets
            .iter()
            .any(|entry| entry["id"].as_i64() == Some(id))
    );

    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "edit",
            &id.to_string(),
            "--title",
            "Read preprocessed BIDS scan",
            "--tag",
            "bids",
            "--body",
            "Use bidser to find a derivative scan, then load it with neuroim2.",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let edited: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(edited["command"], "snippet_edit");
    assert_eq!(edited["payload"]["title"], "Read preprocessed BIDS scan");

    let (code, stdout) = run_with_socket(
        &socket,
        &["snippet", "search", "derivative scan", "--tag", "bids"],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
    assert_eq!(matches[0]["id"].as_i64(), Some(id));
    assert_eq!(
        searched["payload"]["match_query"],
        "\"derivative\" AND \"scan\""
    );

    let (code, stdout) = run_with_socket(&socket, &["snippet", "delete", &id.to_string()]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let deleted: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(deleted["payload"]["deleted"], true);
}

#[test]
fn snippet_show_marks_version_mismatches_as_stale() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-snippet-stale.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    let index_path = socket
        .parent()
        .expect("socket should have parent")
        .join("rpeek-index.sqlite3");

    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "add",
            "--title",
            "Stats note",
            "--package",
            "stats",
            "--status",
            "verified",
            "--body",
            "Call lm for a quick linear model.",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let added: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let id = added["payload"]["id"].as_i64().expect("missing snippet id");

    let conn = Connection::open(&index_path).expect("open index db");
    conn.execute(
        "UPDATE package_records SET version = '999.0.0' WHERE package = 'stats'",
        [],
    )
    .expect("update package version");

    let (code, stdout) = run_with_socket(&socket, &["snippet", "show", &id.to_string()]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let shown: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(shown["payload"]["status"], "verified");
    assert_eq!(shown["payload"]["effective_status"], "stale");
    let stale_packages = shown["payload"]["stale_packages"]
        .as_array()
        .expect("missing stale package list");
    assert_eq!(stale_packages.len(), 1);
    assert_eq!(stale_packages[0]["package"], "stats");
    assert!(stale_packages[0]["recorded_version"].as_str().is_some());
    assert_eq!(stale_packages[0]["current_version"], "999.0.0");

    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "refresh",
            &id.to_string(),
            "--status",
            "verified",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let refreshed: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(refreshed["command"], "snippet_refresh");
    assert_eq!(refreshed["payload"]["status"], "verified");
    assert_eq!(refreshed["payload"]["effective_status"], "verified");
    assert_eq!(
        refreshed["payload"]["stale_packages"]
            .as_array()
            .expect("missing stale package list")
            .len(),
        0
    );
}

#[test]
fn snippet_export_import_round_trips_between_indexes() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let export_path = tempdir.path().join("snippets.json");
    let machine_a = tempdir.path().join("machine-a");
    let machine_b = tempdir.path().join("machine-b");
    fs::create_dir_all(&machine_a).expect("create machine-a dir");
    fs::create_dir_all(&machine_b).expect("create machine-b dir");

    let socket_a = machine_a.join("rpeek-snippet-export-a.sock");
    let _guard_a = DaemonGuard {
        socket: socket_a.clone(),
    };
    let (code, stdout) = run_with_socket(
        &socket_a,
        &[
            "snippet",
            "add",
            "--title",
            "Cross-machine note",
            "--package",
            "stats",
            "--tag",
            "workflow",
            "--body",
            "Call lm on the new machine too.",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");

    let (code, stdout) = run_with_socket(
        &socket_a,
        &[
            "snippet",
            "export",
            "--all",
            "--file",
            export_path.to_str().expect("utf8 export path"),
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let exported: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(exported["command"], "snippet_export");
    assert_eq!(exported["payload"]["count"], 1);
    assert!(export_path.exists());

    let socket_b = machine_b.join("rpeek-snippet-export-b.sock");
    let _guard_b = DaemonGuard {
        socket: socket_b.clone(),
    };
    let (code, stdout) = run_with_socket(
        &socket_b,
        &[
            "snippet",
            "import",
            "--file",
            export_path.to_str().expect("utf8 export path"),
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let imported: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(imported["command"], "snippet_import");
    assert_eq!(imported["payload"]["count"], 1);
    assert_eq!(imported["payload"]["inserted"], 1);
    assert_eq!(imported["payload"]["merged"], 0);

    let (code, stdout) = run_with_socket(&socket_b, &["snippet", "search", "cross-machine"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
    assert_eq!(matches[0]["title"], "Cross-machine note");
    assert_eq!(
        searched["payload"]["match_query"],
        "\"cross\" AND \"machine\""
    );

    let (code, stdout) = run_with_socket(
        &socket_b,
        &[
            "snippet",
            "import",
            "--file",
            export_path.to_str().expect("utf8 export path"),
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let imported_again: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(imported_again["payload"]["count"], 1);
    assert_eq!(imported_again["payload"]["inserted"], 0);
    assert_eq!(imported_again["payload"]["merged"], 1);

    let (code, stdout) = run_with_socket(&socket_b, &["snippet", "list"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let listed: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let snippets = listed["payload"]["snippets"]
        .as_array()
        .expect("missing snippets");
    assert_eq!(snippets.len(), 1);
}

#[test]
fn snippet_search_can_use_raw_match() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-snippet-raw.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    let (code, stdout) = run_with_socket(
        &socket,
        &[
            "snippet",
            "add",
            "--title",
            "Predict note",
            "--tag",
            "workflow",
            "--body",
            "Use predict.lm on a fitted model.",
        ],
    );
    assert_eq!(code, 0, "stdout: {stdout}");

    let (code, stdout) = run_with_socket(
        &socket,
        &["snippet", "search", "\"predict\" OR lm", "--raw-match"],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(searched["payload"]["raw_match"], true);
    assert_eq!(searched["payload"]["match_query"], "\"predict\" OR lm");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
}

#[test]
fn index_clear_resets_persistent_package_state() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-index.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let (code, stdout) = run_with_socket(&socket, &["index", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let status: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(status["payload"]["packages"], 1);

    let (code, stdout) = run_with_socket(&socket, &["index", "clear"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let cleared: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(cleared["command"], "index_clear");
    assert_eq!(cleared["payload"]["cleared_packages"], 1);

    let (code, stdout) = run_with_socket(&socket, &["index", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let status: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(status["payload"]["packages"], 0);
}

#[test]
fn index_package_builds_queryable_package_bundle() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-index-package.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["index", "package", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let indexed: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(indexed["command"], "index_package");
    assert_eq!(indexed["payload"]["package"], "stats");
    assert!(indexed["payload"]["topics_count"].as_u64().unwrap_or(0) > 0);
    assert!(indexed["payload"]["vignettes_count"].as_u64().unwrap_or(0) > 0);
    assert!(indexed["payload"]["files_count"].as_u64().unwrap_or(0) > 0);

    let (code, stdout) = run_with_socket(&socket, &["index", "show", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let shown: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(shown["command"], "index_show");
    assert_eq!(shown["payload"]["package"], "stats");
    assert!(shown["payload"]["exports_count"].as_u64().unwrap_or(0) > 0);

    let (code, stdout) = run_with_socket(&socket, &["index", "search", "stats", "reshape"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(searched["command"], "index_search");
    assert_eq!(searched["payload"]["match_query"], "\"reshape\"");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());
}

#[test]
fn resolve_finds_stats_lm() {
    let (code, stdout) = run(&["resolve", "--kind", "object", "--limit", "5", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "resolve");
    let candidates = value["payload"]["candidates"]
        .as_array()
        .expect("missing candidates");
    assert!(
        candidates
            .iter()
            .any(|entry| entry["package"] == "stats" && entry["name"] == "lm")
    );
}

#[test]
fn grep_searches_package_files() {
    let (code, stdout) = run(&["grep", "--limit", "5", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "grep");
    assert!(value["payload"]["matches"].is_array());
}

#[test]
fn max_bytes_trims_large_strings() {
    let (code, stdout) = run(&["--max-bytes", "80", "doc", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let text = value["payload"]["text"].as_str().expect("missing text");
    assert!(text.contains("[truncated"));
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
fn pkg_request_lazily_builds_indexed_package_bundle() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-lazy-index.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["index", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let before: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(before["payload"]["indexed_packages"], 0);

    let (code, stdout) = run_with_socket(&socket, &["pkg", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let pkg: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(pkg["payload"]["package"], "stats");

    let (code, stdout) = run_with_socket(&socket, &["index", "show", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let shown: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(shown["payload"]["package"], "stats");
    assert!(shown["payload"]["topics_count"].as_u64().unwrap_or(0) > 0);
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
        .env(
            "RPEEK_INDEX_PATH",
            tempdir.path().join("rpeek-index.sqlite3"),
        )
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
        .env(
            "RPEEK_INDEX_PATH",
            tempdir.path().join("rpeek-index.sqlite3"),
        )
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
