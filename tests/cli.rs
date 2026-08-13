use rusqlite::Connection;
use std::fs;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
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
    run_with_socket_and_env(socket, args, &[])
}

fn run_with_socket_and_env(
    socket: &Path,
    args: &[&str],
    extra_env: &[(&str, &Path)],
) -> (i32, String) {
    let index_path = index_path_for_socket(socket);
    let mut command = Command::new(env!("CARGO_BIN_EXE_rpeek"));
    command
        .args(args)
        .env("RPEEK_SOCKET", socket)
        .env("RPEEK_INDEX_PATH", &index_path);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command.output().expect("failed to run rpeek");

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf8");
    (output.status.code().unwrap_or(-1), stdout)
}

fn wait_for_socket(socket: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("daemon socket did not appear: {}", socket.display());
}

fn wait_for_child(child: &mut Child) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if child.try_wait().expect("failed to poll child").is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    panic!("child process did not exit");
}

fn index_path_for_socket(socket: &Path) -> PathBuf {
    socket
        .parent()
        .expect("socket should have parent")
        .join("rpeek-index.sqlite3")
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
fn agent_guidance_includes_index_refresh_recovery() {
    let (code, stdout) = run(&["agent"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "agent");
    let serialized = serde_json::to_string(&value["payload"]).expect("serialize payload");
    assert!(serialized.contains("rpeek index refresh <package>"));
    assert!(serialized.contains("do not switch to Rscript"));
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
fn source_args_only_returns_signature() {
    let (code, stdout) = run(&["source", "--args-only", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["kind"], "signature");
    let text = value["payload"]["text"].as_str().expect("missing text");
    assert!(text.contains("function (formula, data"));
    assert!(!text.contains("ret.x <- x"));
    assert_eq!(value["payload"]["filters"]["args_only"], true);
}

#[test]
fn source_head_caps_lines_and_reports_total() {
    let (code, stdout) = run(&["source", "--head", "5", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let text = value["payload"]["text"].as_str().expect("missing text");
    assert!(text.lines().count() <= 5);
    assert_eq!(value["payload"]["truncated"], true);
    assert_eq!(value["payload"]["filters"]["head"], 5);
    assert!(value["payload"]["total_lines"].as_u64().unwrap_or(0) > 5);
}

#[test]
fn source_grep_filters_to_matching_lines() {
    let (code, stdout) = run(&["source", "--grep", "ret.x", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let text = value["payload"]["text"].as_str().expect("missing text");
    assert!(text.contains("ret.x <- x"));
    assert!(!text.contains("function (formula, data"));
    assert_eq!(value["payload"]["filters"]["grep"], "ret.x");
    assert!(
        value["payload"]["filters"]["grep_match_count"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

#[test]
fn source_context_rejected_without_grep() {
    let (code, stdout) = run(&["source", "--context", "3", "stats", "lm"]);
    assert_ne!(code, 0, "stdout: {stdout}");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["ok"], false);
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("--context")
    );
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
    assert_eq!(value["payload"]["doc_source"], "help");
}

#[test]
fn doc_accepts_pkg_topic_shorthand() {
    let (code, stdout) = run(&["doc", "stats::lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["package"], "stats");
    assert_eq!(value["payload"]["topic"], "lm");
}

#[test]
fn doc_rejects_mixed_pkg_topic_forms() {
    let (code, stdout) = run(&["doc", "stats::lm", "glm"]);
    assert_ne!(code, 0, "stdout: {stdout}");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["ok"], false);
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("ambiguous")
    );
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
    assert!(value["payload"]["generation"].as_str().is_some());
    assert_eq!(value["payload"]["helper_alive"], false);
    assert_eq!(value["payload"]["helper"]["starts"], 0);
    assert!(value["payload"]["cache"]["max_entries"].as_u64().is_some());
}

#[test]
fn mismatched_daemon_generation_is_retired() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-generation.sock");
    let index_path = index_path_for_socket(&socket);
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let mut old_daemon = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .args([
            "serve",
            "--socket",
            socket.to_str().expect("utf8 socket"),
            "--generation",
            "obsolete-test-generation",
        ])
        .env("RPEEK_INDEX_PATH", &index_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start obsolete daemon");
    let old_pid = old_daemon.id();
    wait_for_socket(&socket);

    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let new_pid = value["payload"]["pid"]
        .as_u64()
        .expect("missing daemon pid") as u32;
    assert_ne!(new_pid, old_pid);
    assert_ne!(value["payload"]["generation"], "obsolete-test-generation");
    wait_for_child(&mut old_daemon);
}

#[test]
fn stale_socket_without_daemon_is_recovered() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-stale.sock");
    let listener = UnixListener::bind(&socket).expect("failed to create stale socket fixture");
    drop(listener);
    assert!(socket.exists());
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["payload"]["status"], "running");
    assert!(value["payload"]["generation"].as_str().is_some());
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
    let index_path = index_path_for_socket(&socket);

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
    assert_eq!(indexed["payload"]["freshness"], "refreshed");
    assert_eq!(indexed["payload"]["refresh_reason"], "explicit");
    assert!(indexed["payload"]["topics_count"].as_u64().unwrap_or(0) > 0);
    assert!(indexed["payload"]["vignettes_count"].as_u64().unwrap_or(0) > 0);
    assert!(indexed["payload"]["files_count"].as_u64().unwrap_or(0) > 0);

    let (code, stdout) = run_with_socket(&socket, &["index", "show", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let shown: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(shown["command"], "index_show");
    assert_eq!(shown["payload"]["package"], "stats");
    assert_eq!(shown["payload"]["freshness"], "fresh");
    assert!(shown["payload"]["exports_count"].as_u64().unwrap_or(0) > 0);

    let (code, stdout) = run_with_socket(&socket, &["index", "search", "stats", "reshape"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(searched["command"], "index_search");
    assert_eq!(searched["payload"]["freshness"], "fresh");
    assert_eq!(searched["payload"]["match_query"], "\"reshape\"");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());

    let (code, stdout) = run_with_socket(&socket, &["index", "refresh", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let refreshed: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(refreshed["command"], "index_refresh");
    assert_eq!(refreshed["payload"]["package"], "stats");
    assert_eq!(refreshed["payload"]["freshness"], "refreshed");
    assert_eq!(refreshed["payload"]["refresh_reason"], "explicit");
}

#[test]
fn index_show_builds_missing_package_bundle() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-index-show-refresh.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["index", "show", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let shown: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(shown["command"], "index_show");
    assert_eq!(shown["payload"]["package"], "stats");
    assert_eq!(shown["payload"]["freshness"], "refreshed");
    assert_eq!(shown["payload"]["refresh_reason"], "not_indexed");
    assert!(shown["payload"]["topics_count"].as_u64().unwrap_or(0) > 0);
}

#[test]
fn index_search_refreshes_stale_package_bundle() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-index-search-refresh.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    let index_path = index_path_for_socket(&socket);

    let (code, stdout) = run_with_socket(&socket, &["index", "package", "stats"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let conn = Connection::open(&index_path).expect("open index db");
    conn.execute(
        "UPDATE package_index_state SET local_fingerprint = 'stale-test-fingerprint' WHERE package = 'stats'",
        [],
    )
    .expect("mark package index stale");
    drop(conn);

    let (code, stdout) = run_with_socket(&socket, &["index", "search", "stats", "reshape"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let searched: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(searched["command"], "index_search");
    assert_eq!(searched["payload"]["freshness"], "refreshed");
    assert_eq!(searched["payload"]["refresh_reason"], "fingerprint_changed");
    let matches = searched["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(!matches.is_empty());

    let conn = Connection::open(&index_path).expect("open index db");
    let stored_fingerprint: String = conn
        .query_row(
            "SELECT local_fingerprint FROM package_index_state WHERE package = 'stats'",
            [],
            |row| row.get(0),
        )
        .expect("read refreshed fingerprint");
    assert_ne!(stored_fingerprint, "stale-test-fingerprint");
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
fn grep_searches_deparsed_namespace_objects() {
    let (code, stdout) = run(&[
        "grep", "--scope", "objects", "--limit", "5", "stats", "lm.fit",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["command"], "grep");
    assert_eq!(value["payload"]["scope"], "objects");
    assert_eq!(value["payload"]["scanned_files"], 0);
    assert!(
        value["payload"]["scanned_objects"]
            .as_i64()
            .unwrap_or_default()
            > 0
    );

    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(
        matches
            .iter()
            .any(|entry| entry["kind"] == "object" && entry["object"].as_str().is_some())
    );
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
fn cache_clear_release_stops_helper_but_keeps_daemon() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-cache-release.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let before: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let daemon_pid = before["payload"]["pid"].as_u64().expect("missing pid");
    assert_eq!(before["payload"]["helper_alive"], true);

    let (code, stdout) = run_with_socket(&socket, &["cache", "clear", "--release"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let released: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(released["payload"]["helper_released"], true);

    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let after: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(after["payload"]["pid"], daemon_pid);
    assert_eq!(after["payload"]["helper_alive"], false);
    assert_eq!(after["payload"]["helper"]["manual_releases"], 1);
}

#[test]
fn daemon_reset_helper_releases_helper_and_cached_state() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-reset-helper.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let (code, stdout) = run_with_socket(&socket, &["sig", "stats", "lm"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let (code, stdout) = run_with_socket(&socket, &["daemon", "reset-helper"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let reset: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(reset["command"], "helper_reset");
    assert_eq!(reset["payload"]["status"], "helper_reset");
    assert_eq!(reset["payload"]["helper_released"], true);
    assert_eq!(reset["payload"]["cleared_entries"], 1);

    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let status: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(status["payload"]["helper_alive"], false);
    assert_eq!(status["payload"]["cache"]["entries"], 0);
}

#[test]
fn idle_helper_is_reaped_without_stopping_daemon() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-idle-helper.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    let idle_seconds = Path::new("1");

    let (code, stdout) = run_with_socket_and_env(
        &socket,
        &["sig", "stats", "lm"],
        &[("RPEEK_HELPER_IDLE_SECS", idle_seconds)],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let before: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let daemon_pid = before["payload"]["pid"].as_u64().expect("missing pid");
    assert_eq!(before["payload"]["helper_alive"], true);

    thread::sleep(Duration::from_millis(1_300));
    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let reaped: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(reaped["payload"]["pid"], daemon_pid);
    assert_eq!(reaped["payload"]["helper_alive"], false);
    assert_eq!(reaped["payload"]["helper"]["idle_reaps"], 1);

    let (code, stdout) = run_with_socket(&socket, &["sig", "stats", "glm"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let restarted: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(restarted["payload"]["pid"], daemon_pid);
    assert_eq!(restarted["payload"]["helper_alive"], true);
    assert_eq!(restarted["payload"]["helper"]["starts"], 2);
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
fn search_no_fuzzy_returns_empty_when_no_substring_match() {
    let (code, stdout) = run(&[
        "search",
        "--no-fuzzy",
        "stats",
        "thissymboldoesnotexistxyzzy",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    assert!(
        matches.is_empty(),
        "expected no matches under --no-fuzzy, got: {matches:?}"
    );
}

#[test]
fn search_without_no_fuzzy_falls_back_to_fuzzy() {
    let (code, stdout) = run(&["search", "stats", "thissymboldoesnotexistxyzzy"]);
    assert_eq!(code, 0, "stdout: {stdout}");

    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let matches = value["payload"]["matches"]
        .as_array()
        .expect("missing matches");
    if !matches.is_empty() {
        assert!(
            matches.iter().any(|entry| entry["matched_by"] == "fuzzy"),
            "expected at least one fuzzy match without --no-fuzzy, got: {matches:?}"
        );
    }
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

fn write_reinstall_fixture(source: &Path, version: &str, marker: &str) {
    fs::create_dir_all(source.join("R")).expect("failed to create fixture source");
    fs::write(
        source.join("DESCRIPTION"),
        format!(
            "Package: rpeekfixture\nType: Package\nTitle: rpeek Reinstall Fixture\nVersion: {version}\nAuthors@R: person(\"Test\", \"Author\", email = \"test@example.com\", role = c(\"aut\", \"cre\"))\nDescription: A minimal package used to verify daemon refresh behavior.\nLicense: MIT\nEncoding: UTF-8\n"
        ),
    )
    .expect("failed to write fixture DESCRIPTION");
    fs::write(source.join("NAMESPACE"), "export(probe)\n")
        .expect("failed to write fixture NAMESPACE");
    fs::write(
        source.join("R").join("probe.R"),
        format!("probe <- function(marker = \"{marker}\") marker\n"),
    )
    .expect("failed to write fixture R source");
}

fn install_reinstall_fixture(source: &Path, library: &Path) {
    let output = Command::new("R")
        .arg("CMD")
        .arg("INSTALL")
        .arg(format!("--library={}", library.display()))
        .arg(source)
        .output()
        .expect("failed to run R CMD INSTALL");
    assert!(
        output.status.success(),
        "R CMD INSTALL failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn daemon_restarts_helper_after_installed_package_changes() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let source = tempdir.path().join("rpeekfixture");
    let library = tempdir.path().join("library");
    let socket = tempdir.path().join("rpeek-reinstall.sock");
    fs::create_dir_all(&library).expect("failed to create fixture library");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    write_reinstall_fixture(&source, "0.0.1", "v1");
    install_reinstall_fixture(&source, &library);
    let (code, stdout) = run_with_socket_and_env(
        &socket,
        &["sig", "rpeekfixture", "probe"],
        &[("R_LIBS_USER", &library)],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let first: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert!(
        first["payload"]["signature"]
            .as_str()
            .expect("missing signature")
            .contains("v1")
    );

    write_reinstall_fixture(&source, "0.0.2", "v2");
    install_reinstall_fixture(&source, &library);

    let (code, stdout) = run_with_socket_and_env(
        &socket,
        &["sig", "rpeekfixture", "probe"],
        &[("R_LIBS_USER", &library)],
    );
    assert_eq!(code, 0, "stdout: {stdout}");
    let second: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    let signature = second["payload"]["signature"]
        .as_str()
        .expect("missing signature");
    assert!(signature.contains("v2"), "stale signature: {signature}");
    assert!(!signature.contains("v1"), "stale signature: {signature}");

    let (code, stdout) = run_with_socket(&socket, &["daemon", "status"]);
    assert_eq!(code, 0, "stdout: {stdout}");
    let status: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(status["payload"]["cache"]["invalidations"], 1);
    assert_eq!(status["payload"]["helper"]["restarts"], 1);
}
