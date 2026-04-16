use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use rpeek::index::{
    IndexStore, IndexedFile, IndexedPackageData, IndexedPackageRecord, IndexedSnippet,
    IndexedTopic, IndexedVignette, NewSnippet, PackageIndexState, now_timestamp,
};
use rpeek::protocol::Request;
use rpeek::response::{
    ResponseOptions, apply_response_options, response_exit_code, response_is_success,
    response_reports_success,
};
use rpeek::schema::{SchemaKind, schema_response};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const SOCKET_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTHCHECK_TIMEOUT: Duration = Duration::from_millis(500);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const INDEX_TEXT_FILE_LIMIT: u64 = 512 * 1024;
const HELPER_SCRIPT: &str = include_str!("r_helper.R");
const AFTER_HELP: &str = "\
Examples:
  rpeek search dplyr mutate
  rpeek search --kind topic --limit 5 stats lm
  rpeek search-all lm
  rpeek map dplyr
  rpeek methods-across predict --package stats --package graphics
  rpeek bridge stats graphics
  rpeek xref stats lm
  rpeek used-by graphics plot
  rpeek snippet add --title \"Read BIDS preproc scan\" --package bidser --package neuroim2 --tag workflow --body \"Use bidser to locate scans, then load with neuroim2.\"
  rpeek snippet search \"bids workflow\"
  rpeek sigs dplyr
  rpeek vignettes dplyr
  rpeek vignette dplyr rowwise
  rpeek summary dplyr mutate
  rpeek source dplyr mutate
  rpeek doc dplyr mutate
  rpeek batch --file requests.jsonl
  rpeek agent";

#[derive(Debug, Parser)]
#[command(
    name = "rpeek",
    version,
    about = "Fast installed-R-package introspection for coding agents",
    after_help = AFTER_HELP
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Run one request directly without the daemon"
    )]
    no_daemon: bool,
    #[arg(
        long,
        global = true,
        help = "Trim large string fields to this many bytes"
    )]
    max_bytes: Option<usize>,
    #[arg(
        long,
        global = true,
        help = "Omit examples from documentation payloads"
    )]
    no_examples: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Package metadata and install location")]
    Pkg { package: String },
    #[command(about = "Exported symbols")]
    Exports { package: String },
    #[command(visible_alias = "ls", about = "All objects in the namespace")]
    Objects { package: String },
    #[command(about = "Search objects and help topics by substring")]
    Search {
        package: String,
        query: String,
        #[arg(long, value_enum, default_value_t = SearchKind::All)]
        kind: SearchKind,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    #[command(
        visible_alias = "searchall",
        about = "Search exported symbols and help topics across installed packages"
    )]
    SearchAll {
        query: String,
        #[arg(long, value_enum, default_value_t = SearchKind::All)]
        kind: SearchKind,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    #[command(about = "Resolve likely objects/topics from a query")]
    Resolve {
        query: String,
        #[arg(long)]
        package: Option<String>,
        #[arg(long, value_enum, default_value_t = SearchKind::All)]
        kind: SearchKind,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    #[command(visible_aliases = ["show", "info"], about = "One-call object summary")]
    Summary { package: String, name: String },
    #[command(about = "Function signature or object metadata")]
    Sig { package: String, name: String },
    #[command(about = "One-shot package orientation map for agents")]
    Map { package: String },
    #[command(about = "Find methods for one generic across indexed packages")]
    MethodsAcross {
        generic: String,
        #[arg(long = "package")]
        packages: Vec<String>,
    },
    #[command(about = "Summarize dependency and method overlap between two packages")]
    Bridge {
        package: String,
        other_package: String,
    },
    #[command(about = "Show best-effort symbol xref for one package symbol")]
    Xref { package: String, symbol: String },
    #[command(about = "Show indexed callers of one package symbol across indexed packages")]
    UsedBy { package: String, symbol: String },
    #[command(
        visible_alias = "signatures",
        about = "Function signatures for one package"
    )]
    Sigs {
        package: String,
        #[arg(long, help = "Include non-exported namespace objects")]
        all_objects: bool,
    },
    #[command(visible_alias = "src", about = "Best-effort source retrieval")]
    Source { package: String, name: String },
    #[command(about = "Installed help / roxygen-derived docs")]
    Doc { package: String, topic: String },
    #[command(hide = true, about = "Installed help topic names")]
    Topics { package: String },
    #[command(about = "Related S3/S4 methods")]
    Methods { package: String, name: String },
    #[command(about = "Installed vignette metadata")]
    Vignettes { package: String },
    #[command(about = "Read one installed vignette")]
    Vignette { package: String, name: String },
    #[command(about = "Search installed vignette titles and text")]
    SearchVignettes {
        package: String,
        query: String,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    #[command(about = "Installed package files")]
    Files { package: String },
    #[command(about = "Search installed package files")]
    Grep {
        package: String,
        query: String,
        #[arg(long)]
        glob: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    #[command(about = "Quick usage guide for agents and scripts")]
    Agent,
    #[command(about = "Store and retrieve workflow snippets in the persistent index")]
    Snippet {
        #[command(subcommand)]
        command: SnippetCommands,
    },
    #[command(about = "Check prerequisites (R, jsonlite, etc.)")]
    Doctor,
    #[command(about = "Print JSON schema for request or response contracts")]
    Schema {
        #[arg(value_enum, default_value_t = SchemaKind::Response)]
        kind: SchemaKind,
    },
    #[command(about = "Run multiple JSON requests from stdin or a file")]
    Batch {
        #[arg(long)]
        file: Option<PathBuf>,
    },
    #[command(about = "Inspect or reset the persistent index metadata store")]
    Index {
        #[command(subcommand)]
        command: IndexCommands,
    },
    #[command(hide = true, about = "Stop the background daemon")]
    Shutdown,
    #[command(about = "Inspect or control the background daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },
    #[command(hide = true)]
    Serve {
        #[arg(long)]
        socket: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum CacheCommands {
    Clear,
    Stats,
}

#[derive(Debug, Subcommand)]
enum IndexCommands {
    #[command(about = "Show persistent index metadata status")]
    Status,
    #[command(about = "Clear persistent index metadata")]
    Clear,
    #[command(about = "Index one installed package into the persistent store")]
    Package { package: String },
    #[command(about = "Show persistent indexed content counts for one package")]
    Show { package: String },
    #[command(about = "Search indexed docs, vignettes, examples, and files for one package")]
    Search {
        package: String,
        query: String,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
}

#[derive(Debug, Subcommand)]
enum SnippetCommands {
    #[command(about = "Add one indexed workflow snippet")]
    Add {
        #[arg(long)]
        title: String,
        #[arg(long = "package")]
        packages: Vec<String>,
        #[arg(long = "object")]
        objects: Vec<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long = "verb")]
        verbs: Vec<String>,
        #[arg(long, value_enum, default_value_t = SnippetStatusArg::Unknown)]
        status: SnippetStatusArg,
        #[arg(long)]
        source: Option<String>,
        #[arg(long, conflicts_with = "body")]
        file: Option<PathBuf>,
        #[arg(long, conflicts_with = "file")]
        body: Option<String>,
    },
    #[command(about = "List stored snippets")]
    List {
        #[arg(long = "package")]
        package: Option<String>,
        #[arg(long = "tag")]
        tag: Option<String>,
        #[arg(long, value_enum)]
        status: Option<SnippetStatusArg>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    #[command(about = "Show one stored snippet")]
    Show { id: i64 },
    #[command(about = "Refresh recorded package versions for one snippet")]
    Refresh {
        id: i64,
        #[arg(long, value_enum)]
        status: Option<SnippetStatusArg>,
    },
    #[command(about = "Search stored snippets with bm25-ranked FTS")]
    Search {
        query: String,
        #[arg(long = "package")]
        package: Option<String>,
        #[arg(long = "tag")]
        tag: Option<String>,
        #[arg(long, value_enum)]
        status: Option<SnippetStatusArg>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    #[command(about = "Delete one stored snippet")]
    Delete { id: i64 },
}

#[derive(Debug, Subcommand)]
enum DaemonCommands {
    #[command(about = "Show daemon status")]
    Status,
    #[command(about = "Stop the daemon")]
    Stop,
    #[command(about = "Restart the daemon")]
    Restart,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SearchKind {
    All,
    Object,
    Topic,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StalePackageInfo {
    package: String,
    recorded_version: Option<String>,
    current_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnippetStatusEvaluation {
    effective_status: String,
    stale_packages: Vec<StalePackageInfo>,
    current_package_versions: BTreeMap<String, String>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SnippetStatusArg {
    Unknown,
    Verified,
    Stale,
    Failed,
}

impl SnippetStatusArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Verified => "verified",
            Self::Stale => "stale",
            Self::Failed => "failed",
        }
    }
}

impl SearchKind {
    fn as_request_value(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Object => "object",
            Self::Topic => "topic",
        }
    }
}

fn main() {
    let exit_code = match run() {
        Ok(value) => {
            let exit_code = response_exit_code(&value);
            println!("{value}");
            exit_code
        }
        Err(err) => {
            let error = json!({
                "schema_version": 1,
                "ok": false,
                "error": {
                    "code": "client_error",
                    "message": err.to_string(),
                }
            });
            println!("{error}");
            1
        }
    };

    std::process::exit(exit_code);
}

fn run() -> Result<Value> {
    let cli = Cli::parse();
    let response_options = ResponseOptions {
        max_bytes: cli.max_bytes,
        no_examples: cli.no_examples,
    };
    match cli.command {
        Commands::Serve { socket } => {
            serve(socket)?;
            Ok(json!({
                "schema_version": 1,
                "ok": true,
                "command": "serve",
                "payload": { "status": "daemon_stopped" }
            }))
        }
        Commands::Agent => Ok(agent_response()),
        Commands::Snippet { command } => Ok(snippet_response(command)?),
        Commands::Doctor => Ok(doctor_response()),
        Commands::Schema { kind } => Ok(schema_response(kind)),
        Commands::Batch { file } => Ok(batch_response(file, &response_options)?),
        Commands::Index {
            command: IndexCommands::Status,
        } => Ok(index_status_response()?),
        Commands::Index {
            command: IndexCommands::Clear,
        } => Ok(index_clear_response()?),
        Commands::Index {
            command: IndexCommands::Package { package },
        } => Ok(index_package_response(&package)?),
        Commands::Index {
            command: IndexCommands::Show { package },
        } => Ok(index_show_response(&package)?),
        Commands::Index {
            command:
                IndexCommands::Search {
                    package,
                    query,
                    limit,
                },
        } => Ok(index_search_response(&package, &query, limit)?),
        Commands::MethodsAcross { generic, packages } => {
            Ok(methods_across_response(&generic, &packages)?)
        }
        Commands::Bridge {
            package,
            other_package,
        } => Ok(bridge_response(&package, &other_package)?),
        Commands::Xref { package, symbol } => Ok(xref_response(&package, &symbol)?),
        Commands::UsedBy { package, symbol } => Ok(used_by_response(&package, &symbol)?),
        Commands::Daemon {
            command: DaemonCommands::Restart,
        } => {
            let _ = query_daemon(&Request::Shutdown, &ResponseOptions::default());
            let mut value = query_daemon(&Request::DaemonStatus, &response_options)?;
            if let Some(map) = value.as_object_mut() {
                map.insert(
                    "command".to_string(),
                    Value::String("daemon_restart".to_string()),
                );
            }
            Ok(value)
        }
        command => {
            let request = request_from_command(command)?;
            if cli.no_daemon && request.can_run_without_daemon() {
                query_helper_once(&request, &response_options)
            } else {
                query_daemon(&request, &response_options)
            }
        }
    }
}

fn request_from_command(command: Commands) -> Result<Request> {
    let request = match command {
        Commands::Pkg { package } => Request::Pkg { package },
        Commands::Exports { package } => Request::Exports { package },
        Commands::Objects { package } => Request::Objects { package },
        Commands::Search {
            package,
            query,
            kind,
            limit,
        } => Request::Search {
            package,
            query,
            kind: kind.as_request_value().to_string(),
            limit,
        },
        Commands::SearchAll { query, kind, limit } => Request::SearchAll {
            query,
            kind: kind.as_request_value().to_string(),
            limit,
        },
        Commands::Resolve {
            query,
            package,
            kind,
            limit,
        } => Request::Resolve {
            query,
            package,
            kind: kind.as_request_value().to_string(),
            limit,
        },
        Commands::Summary { package, name } => Request::Summary { package, name },
        Commands::Sig { package, name } => Request::Sig { package, name },
        Commands::Map { package } => Request::Map { package },
        Commands::MethodsAcross { .. } => bail!("methods-across is handled directly"),
        Commands::Bridge { .. } => bail!("bridge is handled directly"),
        Commands::Xref { .. } => bail!("xref is handled directly"),
        Commands::UsedBy { .. } => bail!("used-by is handled directly"),
        Commands::Sigs {
            package,
            all_objects,
        } => Request::Sigs {
            package,
            all_objects,
        },
        Commands::Source { package, name } => Request::Source { package, name },
        Commands::Doc { package, topic } => Request::Doc { package, topic },
        Commands::Topics { package } => Request::Topics { package },
        Commands::Methods { package, name } => Request::Methods { package, name },
        Commands::Vignettes { package } => Request::Vignettes { package },
        Commands::Vignette { package, name } => Request::Vignette { package, name },
        Commands::SearchVignettes {
            package,
            query,
            limit,
        } => Request::SearchVignettes {
            package,
            query,
            limit,
        },
        Commands::Files { package } => Request::Files { package },
        Commands::Grep {
            package,
            query,
            glob,
            limit,
        } => Request::Grep {
            package,
            query,
            glob,
            limit,
        },
        Commands::Cache { command } => match command {
            CacheCommands::Clear => Request::CacheClear,
            CacheCommands::Stats => Request::CacheStats,
        },
        Commands::Index { .. } => bail!("index is not a daemon command"),
        Commands::Agent => bail!("agent is not a daemon command"),
        Commands::Snippet { .. } => bail!("snippet is handled directly"),
        Commands::Batch { .. } => bail!("batch is not a daemon command"),
        Commands::Schema { .. } => bail!("schema is not a daemon command"),
        Commands::Shutdown => Request::Shutdown,
        Commands::Daemon { command } => match command {
            DaemonCommands::Status => Request::DaemonStatus,
            DaemonCommands::Stop => Request::Shutdown,
            DaemonCommands::Restart => bail!("daemon restart is handled directly"),
        },
        Commands::Serve { .. } => bail!("serve is not a client command"),
        Commands::Doctor => bail!("doctor is not a client command"),
    };

    Ok(request)
}

fn query_daemon(request: &Request, options: &ResponseOptions) -> Result<Value> {
    let socket = socket_path();
    if matches!(request, Request::Shutdown) && !socket.exists() {
        return Ok(json!({
            "schema_version": 1,
            "ok": true,
            "command": "shutdown",
            "payload": { "status": "not_running" }
        }));
    }

    ensure_daemon_running(&socket)?;

    let line = serde_json::to_string(request)?;
    let response = send_request_with_retry(&socket, &line)?;
    let response = response.trim();

    let mut value: Value = serde_json::from_str(response)
        .with_context(|| format!("invalid JSON response from daemon: {response}"))?;
    if let Some(map) = value.as_object_mut() {
        map.insert(
            "command".to_string(),
            Value::String(request.action().to_string()),
        );
    }
    apply_response_options(&mut value, options);
    Ok(value)
}

fn query_helper_once(request: &Request, options: &ResponseOptions) -> Result<Value> {
    let socket = env::temp_dir().join(format!("rpeek-oneshot-{}.sock", std::process::id()));
    let mut helper = HelperProcess::start(&socket)?;
    let line = serde_json::to_string(request)?;
    let response = helper.send(&line)?;
    let mut value: Value = serde_json::from_str(response.trim())
        .with_context(|| format!("invalid JSON response from helper: {response}"))?;
    if let Some(map) = value.as_object_mut() {
        map.insert(
            "command".to_string(),
            Value::String(request.action().to_string()),
        );
    }
    apply_response_options(&mut value, options);
    Ok(value)
}

fn query_helper_value(
    helper: &mut HelperProcess,
    socket: &Path,
    request: &Request,
) -> Result<Value> {
    let line = serde_json::to_string(request)?;
    let response = helper
        .send(&line)
        .or_else(|_| helper.restart(socket, &line))
        .context("failed to query R helper")?;
    let value: Value = serde_json::from_str(response.trim())
        .with_context(|| format!("invalid JSON response from helper: {response}"))?;
    if !response_reports_success(&value) {
        let error = value
            .get("error")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("helper_error");
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("R helper request failed");
        bail!("{code}: {message}");
    }
    Ok(value)
}

fn query_helper_payload(
    helper: &mut HelperProcess,
    socket: &Path,
    request: &Request,
) -> Result<Value> {
    let value = query_helper_value(helper, socket, request)?;
    value
        .get("payload")
        .cloned()
        .ok_or_else(|| anyhow!("helper response missing payload"))
}

fn ensure_daemon_running(socket: &Path) -> Result<()> {
    if socket.exists() {
        return Ok(());
    }

    let lock_path = socket_lock_path(socket);
    let spawn_lock = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&lock_path);

    let acquired_lock = spawn_lock.is_ok();
    if acquired_lock {
        if socket.exists() {
            let _ = fs::remove_file(socket);
        }

        let current_exe = env::current_exe().context("failed to resolve current executable")?;
        let mut command = Command::new(current_exe);
        command
            .arg("serve")
            .arg("--socket")
            .arg(socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command.spawn().context("failed to spawn rpeek daemon")?;
    }

    let start = Instant::now();
    while start.elapsed() < SOCKET_WAIT_TIMEOUT {
        if daemon_is_healthy(socket) {
            if acquired_lock {
                let _ = fs::remove_file(&lock_path);
            }
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }

    if acquired_lock {
        let _ = fs::remove_file(&lock_path);
    }

    bail!(
        "daemon did not become ready within {}ms",
        SOCKET_WAIT_TIMEOUT.as_millis()
    );
}

fn daemon_is_healthy(socket: &Path) -> bool {
    if !socket.exists() {
        return false;
    }

    let ping = serde_json::to_string(&Request::Ping);
    let Ok(ping) = ping else {
        return false;
    };

    let Ok(response) = send_request_line(socket, &ping, HEALTHCHECK_TIMEOUT) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(&response) else {
        return false;
    };

    value.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

fn send_request_line(socket: &Path, line: &str, timeout: Duration) -> Result<String> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("failed to connect to daemon at {}", socket.display()))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    if response.trim().is_empty() {
        bail!("daemon returned an empty response");
    }
    Ok(response)
}

fn send_request_with_retry(socket: &Path, line: &str) -> Result<String> {
    match send_request_line(socket, line, REQUEST_TIMEOUT) {
        Ok(response) => Ok(response),
        Err(_) => {
            recover_daemon(socket)?;
            ensure_daemon_running(socket)?;
            send_request_line(socket, line, REQUEST_TIMEOUT)
        }
    }
}

fn recover_daemon(socket: &Path) -> Result<()> {
    if daemon_is_healthy(socket) {
        bail!("daemon is healthy but the request failed or timed out");
    }

    let _ = send_shutdown(socket);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        if !socket.exists() {
            return Ok(());
        }
        if !daemon_is_healthy(socket) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    if socket.exists() {
        let _ = fs::remove_file(socket);
    }
    Ok(())
}

fn send_shutdown(socket: &Path) -> Result<String> {
    let line = serde_json::to_string(&Request::Shutdown)?;
    send_request_line(socket, &line, HEALTHCHECK_TIMEOUT)
}

fn serve(socket: PathBuf) -> Result<()> {
    if socket.exists() {
        let _ = fs::remove_file(&socket);
    }

    if let Some(parent) = socket.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create socket directory {}", parent.display()))?;
    }

    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("failed to bind daemon socket at {}", socket.display()))?;
    let mut helper = HelperProcess::start(&socket)?;
    let mut cache = ResponseCache::new()?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut should_shutdown = false;
                let response = match read_request_line(&mut stream).and_then(|line| {
                    let request: Request =
                        serde_json::from_str(&line).context("failed to parse client request")?;
                    should_shutdown = matches!(request, Request::Shutdown);
                    handle_request(&request, &line, &mut helper, &mut cache, &socket)
                }) {
                    Ok(response) => response,
                    Err(err) => json!({
                        "schema_version": 1,
                        "ok": false,
                        "error": {
                            "code": "helper_error",
                            "message": err.to_string()
                        }
                    })
                    .to_string(),
                };

                let _ = stream.write_all(response.as_bytes());
                if should_shutdown {
                    break;
                }
            }
            Err(err) => return Err(err).context("failed to accept daemon connection"),
        }
    }

    let _ = fs::remove_file(&socket);
    Ok(())
}

fn handle_request(
    request: &Request,
    line: &str,
    helper: &mut HelperProcess,
    cache: &mut ResponseCache,
    socket: &Path,
) -> Result<String> {
    match request {
        Request::Ping => Ok(json!({
            "schema_version": 1,
            "ok": true,
            "payload": { "status": "ok" }
        })
        .to_string()),
        Request::DaemonStatus => Ok(daemon_status_response(cache, helper, socket)),
        Request::Shutdown => Ok(json!({
            "schema_version": 1,
            "ok": true,
            "payload": { "status": "daemon_stopping" }
        })
        .to_string()),
        Request::CacheClear => Ok(cache.clear_response()),
        Request::CacheStats => Ok(cache.stats_response()),
        _ => handle_query_request(request, line, helper, cache, socket),
    }
}

fn daemon_status_response(
    cache: &ResponseCache,
    helper: &mut HelperProcess,
    socket: &Path,
) -> String {
    json!({
        "schema_version": 1,
        "ok": true,
        "payload": {
            "status": "running",
            "pid": std::process::id(),
            "socket": socket.display().to_string(),
            "helper_alive": helper.is_alive(),
            "cache": cache.stats_payload(),
            "index": cache.index_payload()
        }
    })
    .to_string()
}

fn handle_query_request(
    request: &Request,
    line: &str,
    helper: &mut HelperProcess,
    cache: &mut ResponseCache,
    socket: &Path,
) -> Result<String> {
    if request.requires_package() && request.package().is_none() {
        bail!("request is missing package");
    }

    if request.is_cacheable() {
        let package = request
            .package()
            .ok_or_else(|| anyhow!("cacheable request is missing package"))?;
        let fingerprint = match cache.resolve_fingerprint(package, helper, socket)? {
            FingerprintResolution::Fingerprint(fingerprint) => fingerprint,
            FingerprintResolution::ErrorResponse(response) => return Ok(response),
        };
        let key = CacheKey {
            request: request.clone(),
            fingerprint,
        };

        if let Some(response) = cache.get(&key) {
            return Ok(response);
        }

        if request.can_use_index() {
            cache.ensure_indexed_package(package, &key.fingerprint, helper, socket)?;
            if let Some(response) = cache.indexed_response(request)? {
                if response_is_success(&response) {
                    cache.insert(key, response.clone());
                }
                return Ok(response);
            }
        }

        let response = helper
            .send(line)
            .or_else(|_| helper.restart(socket, line))
            .context("failed to query R helper")?;
        if response_is_success(&response) {
            cache.insert(key, response.clone());
        }
        return Ok(response);
    }

    helper
        .send(line)
        .or_else(|_| helper.restart(socket, line))
        .context("failed to query R helper")
}

fn indexed_request_response(cache: &ResponseCache, request: &Request) -> Result<Option<String>> {
    let package = match request.package() {
        Some(package) => package,
        None => return Ok(None),
    };
    let Some(package_data) = cache.index.get_indexed_package_data(package)? else {
        return Ok(None);
    };

    let payload = match request {
        Request::Pkg { .. } => package_data.package_json.clone(),
        Request::Exports { .. } => json!({
            "package": package,
            "exports": package_data.exports,
        }),
        Request::Objects { .. } => json!({
            "package": package,
            "objects": package_data.objects,
        }),
        Request::Search {
            query, kind, limit, ..
        } => indexed_search_payload(
            &package_data,
            &cache.index.get_indexed_topics(package)?,
            query,
            kind,
            *limit,
        ),
        Request::Map { .. } => indexed_map_payload(
            &package_data,
            &cache.index.get_indexed_topics(package)?,
            &cache.index.get_indexed_vignettes(package)?,
            &cache.index.get_indexed_files(package)?,
        ),
        Request::Sigs { all_objects, .. } if !all_objects => {
            let signatures = package_data
                .signatures_json
                .as_array()
                .cloned()
                .unwrap_or_default();
            json!({
                "package": package,
                "all_objects": false,
                "counts": {
                    "scanned": package_data.objects.len(),
                    "signatures": signatures.len(),
                },
                "signatures": signatures,
            })
        }
        Request::Topics { .. } => {
            let topics = cache
                .index
                .get_indexed_topics(package)?
                .into_iter()
                .map(|topic| topic.topic)
                .collect::<Vec<_>>();
            json!({
                "package": package,
                "topics": topics,
            })
        }
        Request::Vignettes { .. } => {
            let vignettes = cache
                .index
                .get_indexed_vignettes(package)?
                .into_iter()
                .map(vignette_listing_json)
                .collect::<Vec<_>>();
            json!({
                "package": package,
                "vignettes": vignettes,
                "counts": {
                    "vignettes": vignettes.len(),
                }
            })
        }
        Request::Vignette { name, .. } => {
            let vignettes = cache.index.get_indexed_vignettes(package)?;
            let Some(vignette) = match_vignette(&vignettes, name) else {
                return Ok(None);
            };
            vignette_detail_json(package, &vignette)
        }
        Request::SearchVignettes { query, limit, .. } => indexed_search_vignettes_payload(
            package,
            &cache.index.get_indexed_vignettes(package)?,
            query,
            *limit,
        ),
        _ => return Ok(None),
    };

    Ok(Some(
        json!({
            "schema_version": 1,
            "ok": true,
            "payload": payload
        })
        .to_string(),
    ))
}

fn read_request_line(stream: &mut UnixStream) -> Result<String> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let count = reader.read_line(&mut line)?;
    if count == 0 {
        bail!("client sent an empty request");
    }
    Ok(line.trim().to_string())
}

struct HelperProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: BufReader<ChildStderr>,
    script_path: PathBuf,
}

impl HelperProcess {
    fn start(socket: &Path) -> Result<Self> {
        let script_path = helper_script_path(socket);
        fs::write(&script_path, HELPER_SCRIPT)
            .with_context(|| format!("failed to write helper script {}", script_path.display()))?;

        let mut child = Command::new(r_command())
            .arg("--vanilla")
            .arg("--slave")
            .arg("-f")
            .arg(&script_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    anyhow!(
                        "R not found. Install R from https://cran.r-project.org/ and ensure '{}' is on your PATH, \
                         or set RPEEK_R_COMMAND to the full path. Run `rpeek doctor` to diagnose.",
                        r_command()
                    )
                } else {
                    anyhow!("failed to start R helper process: {e}")
                }
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open stdin for R helper"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to open stdout for R helper"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("failed to open stderr for R helper"))?;

        let mut helper = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr: BufReader::new(stderr),
            script_path,
        };
        let ping = serde_json::to_string(&Request::Ping)?;
        helper.send(&ping).context(
            "R helper failed startup ping. Is the 'jsonlite' R package installed? Run `rpeek doctor` to diagnose."
        )?;
        Ok(helper)
    }

    fn restart(&mut self, socket: &Path, request: &str) -> Result<String> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let replacement = Self::start(socket)?;
        *self = replacement;
        self.send(request)
    }

    fn send(&mut self, request: &str) -> Result<String> {
        self.stdin.write_all(request.as_bytes())?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;

        let mut response = String::new();
        let count = self.stdout.read_line(&mut response)?;
        if count == 0 {
            if let Some(status) = self.child.try_wait()? {
                let stderr = self.read_stderr();
                if stderr.is_empty() {
                    bail!("R helper exited before replying with status {status}");
                }
                bail!(
                    "R helper exited before replying with status {status}: {}",
                    stderr.trim()
                );
            }
            bail!("R helper exited before replying");
        }

        Ok(response.trim().to_string())
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    fn read_stderr(&mut self) -> String {
        let mut stderr = String::new();
        let _ = self.stderr.read_to_string(&mut stderr);
        stderr
    }
}

impl Drop for HelperProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_file(&self.script_path);
    }
}

fn socket_path() -> PathBuf {
    if let Ok(path) = env::var("RPEEK_SOCKET") {
        return PathBuf::from(path);
    }
    if let Ok(path) = env::var("RPKG_SOCKET") {
        return PathBuf::from(path);
    }

    let user = env::var("USER").unwrap_or_else(|_| "user".to_string());
    let build_tag = env::current_exe()
        .ok()
        .and_then(|path| fs::metadata(path).ok())
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|| "dev".to_string());
    env::temp_dir().join(format!("rpeek-{user}-{build_tag}.sock"))
}

fn helper_script_path(socket: &Path) -> PathBuf {
    let stem = socket
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("rpeek");
    socket.with_file_name(format!("{stem}-helper.R"))
}

fn socket_lock_path(socket: &Path) -> PathBuf {
    let stem = socket
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("rpeek.sock");
    socket.with_file_name(format!("{stem}.lock"))
}

fn r_command() -> String {
    env::var("RPEEK_R_COMMAND")
        .or_else(|_| env::var("RPKG_R_COMMAND"))
        .unwrap_or_else(|_| "R".to_string())
}

fn doctor_response() -> Value {
    let r_cmd = r_command();
    let mut checks = Vec::new();
    let mut all_ok = true;

    // Check R on PATH
    match Command::new(&r_cmd)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => {
            let text = String::from_utf8_lossy(&output.stdout);
            let stderr_text = String::from_utf8_lossy(&output.stderr);
            // R --version prints to stdout on some platforms, stderr on others
            let version_text = if text.contains("R version") {
                text.to_string()
            } else {
                stderr_text.to_string()
            };
            let version_line = version_text
                .lines()
                .find(|l| l.contains("R version"))
                .unwrap_or("unknown version")
                .trim()
                .to_string();
            checks.push(json!({
                "check": "R",
                "ok": true,
                "detail": version_line,
                "command": r_cmd,
            }));
        }
        Err(_) => {
            all_ok = false;
            checks.push(json!({
                "check": "R",
                "ok": false,
                "detail": format!("'{}' not found on PATH", r_cmd),
                "fix": "Install R from https://cran.r-project.org/ and ensure it is on your PATH.\nOr set RPEEK_R_COMMAND to the full path of your R binary.",
            }));
        }
    }

    // Check jsonlite
    let jsonlite_ok = Command::new(&r_cmd)
        .arg("--vanilla")
        .arg("--slave")
        .arg("-e")
        .arg("cat(requireNamespace('jsonlite', quietly=TRUE))")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "TRUE")
        .unwrap_or(false);

    if jsonlite_ok {
        checks.push(json!({
            "check": "jsonlite",
            "ok": true,
            "detail": "installed",
        }));
    } else if all_ok {
        // Only report jsonlite if R itself was found
        all_ok = false;
        checks.push(json!({
            "check": "jsonlite",
            "ok": false,
            "detail": "R package 'jsonlite' is not installed",
            "fix": "Rscript -e 'install.packages(\"jsonlite\")'",
        }));
    }

    json!({
        "schema_version": 1,
        "ok": all_ok,
        "command": "doctor",
        "payload": {
            "checks": checks,
            "status": if all_ok { "all prerequisites met" } else { "action required — see fix fields above" },
        }
    })
}

fn agent_response() -> Value {
    json!({
        "schema_version": 1,
        "ok": true,
        "command": "agent",
        "payload": {
            "workflows": [
                {
                    "task": "Get a one-shot package orientation map",
                    "command": "rpeek map <package>"
                },
                {
                    "task": "Find methods for one generic across packages",
                    "command": "rpeek methods-across <generic> --package <pkg>..."
                },
                {
                    "task": "Compare two packages' dependency and method overlap",
                    "command": "rpeek bridge <package> <other-package>"
                },
                {
                    "task": "Trace one symbol's local and cross-package usage",
                    "command": "rpeek xref <package> <symbol>"
                },
                {
                    "task": "Find indexed callers of one symbol",
                    "command": "rpeek used-by <package> <symbol>"
                },
                {
                    "task": "Find likely symbols",
                    "command": "rpeek search <package> <query>"
                },
                {
                    "task": "Limit or filter search results",
                    "command": "rpeek search --kind topic --limit 5 <package> <query>"
                },
                {
                    "task": "Find a symbol when you do not know the package",
                    "command": "rpeek search-all <query>"
                },
                {
                    "task": "Resolve likely objects/topics from an unknown query",
                    "steps": [
                        "rpeek resolve <query>",
                        "rpeek summary <package> <object>",
                        "rpeek doc <package> <topic>"
                    ]
                },
                {
                    "task": "List installed package vignettes",
                    "command": "rpeek vignettes <package>"
                },
                {
                    "task": "Read one installed vignette",
                    "command": "rpeek vignette <package> <name>"
                },
                {
                    "task": "Search installed vignette titles and text",
                    "command": "rpeek search-vignettes <package> <query>"
                },
                {
                    "task": "Get function signatures for one package",
                    "command": "rpeek sigs <package>"
                },
                {
                    "task": "Get one-call object summary",
                    "command": "rpeek summary <package> <object>"
                },
                {
                    "task": "Read source",
                    "command": "rpeek source <package> <object>"
                },
                {
                    "task": "Read docs",
                    "command": "rpeek doc <package> <topic>"
                },
                {
                    "task": "Search installed package files",
                    "command": "rpeek grep <package> <query>"
                },
                {
                    "task": "Avoid daemon reuse for isolated checks",
                    "command": "rpeek --no-daemon summary <package> <object>"
                },
                {
                    "task": "Run multiple requests",
                    "command": "rpeek batch --file requests.jsonl"
                }
            ],
            "notes": [
                "JSON is the default output format.",
                "Use RPEEK_SOCKET=/tmp/<name>.sock to reuse one warm daemon across calls.",
                "Source kind can be raw_file, deparsed, or unavailable.",
                "Use search-all for exported symbols and help topics when the package is unknown.",
                "Use --max-bytes and --no-examples to keep large payloads compact.",
                "Batch input is JSON Lines matching the request schema.",
                "Use rpeek schema request or rpeek schema response to inspect the JSON contract."
            ]
        }
    })
}

fn index_package_response(package: &str) -> Result<Value> {
    let socket = env::temp_dir().join(format!(
        "rpeek-index-build-{}-{}.sock",
        std::process::id(),
        now_timestamp().unwrap_or_default()
    ));
    let mut helper = HelperProcess::start(&socket)?;
    let record = build_indexed_package_record(package, &mut helper, &socket)?;
    let mut store = IndexStore::open_default()?;
    let fingerprint = package_fingerprint(&record.install_path)?;
    let helper_fingerprint = record.version.clone();
    let package = record.package.clone();
    let indexed_at = record.indexed_at;

    store.upsert_package_record(&record)?;
    store.upsert_package_state(&PackageIndexState {
        package: package.clone(),
        install_path: record.install_path.clone(),
        helper_fingerprint,
        local_fingerprint: fingerprint,
        updated_at: indexed_at,
    })?;

    let summary = store
        .get_indexed_package_summary(&package)?
        .ok_or_else(|| anyhow!("indexed package summary missing after write"))?;

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "index_package",
        "payload": summary
    }))
}

fn index_show_response(package: &str) -> Result<Value> {
    let store = IndexStore::open_default()?;
    let summary = store
        .get_indexed_package_summary(package)?
        .ok_or_else(|| anyhow!("package '{package}' is not indexed"))?;
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "index_show",
        "payload": summary
    }))
}

fn index_search_response(package: &str, query: &str, limit: usize) -> Result<Value> {
    let store = IndexStore::open_default()?;
    let matches = store.search_package_documents(package, query, limit)?;
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "index_search",
        "payload": {
            "package": package,
            "query": query,
            "limit": limit,
            "matches": matches,
            "count": matches.len()
        }
    }))
}

fn snippet_response(command: SnippetCommands) -> Result<Value> {
    match command {
        SnippetCommands::Add {
            title,
            packages,
            objects,
            tags,
            verbs,
            status,
            source,
            file,
            body,
        } => snippet_add_response(
            &title,
            &packages,
            &objects,
            &tags,
            &verbs,
            status,
            source,
            file.as_deref(),
            body.as_deref(),
        ),
        SnippetCommands::List {
            package,
            tag,
            status,
            limit,
        } => snippet_list_response(
            package.as_deref(),
            tag.as_deref(),
            status.map(SnippetStatusArg::as_str),
            limit,
        ),
        SnippetCommands::Show { id } => snippet_show_response(id),
        SnippetCommands::Refresh { id, status } => {
            snippet_refresh_response(id, status.map(SnippetStatusArg::as_str))
        }
        SnippetCommands::Search {
            query,
            package,
            tag,
            status,
            limit,
        } => snippet_search_response(
            &query,
            package.as_deref(),
            tag.as_deref(),
            status.map(SnippetStatusArg::as_str),
            limit,
        ),
        SnippetCommands::Delete { id } => snippet_delete_response(id),
    }
}

fn snippet_add_response(
    title: &str,
    packages: &[String],
    objects: &[String],
    tags: &[String],
    verbs: &[String],
    status: SnippetStatusArg,
    source: Option<String>,
    file: Option<&Path>,
    body: Option<&str>,
) -> Result<Value> {
    let body = snippet_body_input(file, body)?;
    let package_versions = indexed_package_versions(packages)?;
    let mut store = IndexStore::open_default()?;
    let snippet = store.add_snippet(&NewSnippet {
        title: title.to_string(),
        body,
        packages: packages.to_vec(),
        objects: objects.to_vec(),
        tags: tags.to_vec(),
        verbs: verbs.to_vec(),
        status: status.as_str().to_string(),
        source,
        package_versions,
    })?;
    let current_versions = current_package_versions_for_snippets(std::slice::from_ref(&snippet))?;
    let evaluation = evaluate_snippet_status(&snippet, &current_versions);

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "snippet_add",
        "payload": snippet_json(&snippet, &evaluation),
    }))
}

fn snippet_list_response(
    package: Option<&str>,
    tag: Option<&str>,
    status: Option<&str>,
    limit: usize,
) -> Result<Value> {
    let store = IndexStore::open_default()?;
    let snippets = store.list_snippets(package, tag, status, limit)?;
    let current_versions = current_package_versions_for_snippets(&snippets)?;
    let items = snippets
        .iter()
        .map(|snippet| {
            let evaluation = evaluate_snippet_status(snippet, &current_versions);
            snippet_summary_json(snippet, &evaluation)
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "snippet_list",
        "payload": {
            "filters": {
                "package": package,
                "tag": tag,
                "status": status,
            },
            "count": items.len(),
            "snippets": items,
        }
    }))
}

fn snippet_show_response(id: i64) -> Result<Value> {
    let store = IndexStore::open_default()?;
    let snippet = store
        .get_snippet(id)?
        .ok_or_else(|| anyhow!("snippet '{id}' was not found"))?;
    let current_versions = current_package_versions_for_snippets(std::slice::from_ref(&snippet))?;
    let evaluation = evaluate_snippet_status(&snippet, &current_versions);
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "snippet_show",
        "payload": snippet_json(&snippet, &evaluation),
    }))
}

fn snippet_refresh_response(id: i64, status: Option<&str>) -> Result<Value> {
    let mut store = IndexStore::open_default()?;
    let snippet = store
        .get_snippet(id)?
        .ok_or_else(|| anyhow!("snippet '{id}' was not found"))?;
    let package_versions = indexed_package_versions(&snippet.packages)?;
    let refreshed = store
        .refresh_snippet(id, &package_versions, status)?
        .ok_or_else(|| anyhow!("snippet '{id}' disappeared during refresh"))?;
    let current_versions = current_package_versions_for_snippets(std::slice::from_ref(&refreshed))?;
    let evaluation = evaluate_snippet_status(&refreshed, &current_versions);

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "snippet_refresh",
        "payload": snippet_json(&refreshed, &evaluation),
    }))
}

fn snippet_search_response(
    query: &str,
    package: Option<&str>,
    tag: Option<&str>,
    status: Option<&str>,
    limit: usize,
) -> Result<Value> {
    let store = IndexStore::open_default()?;
    let matches = store.search_snippets(query, package, tag, status, limit)?;
    let snippet_rows = matches
        .iter()
        .filter_map(|entry| entry.key.parse::<i64>().ok())
        .filter_map(|id| store.get_snippet(id).ok().flatten())
        .collect::<Vec<_>>();
    let current_versions = current_package_versions_for_snippets(&snippet_rows)?;
    let snippets_by_id = snippet_rows
        .into_iter()
        .map(|snippet| (snippet.id, snippet))
        .collect::<HashMap<_, _>>();
    let results = matches
        .into_iter()
        .filter_map(|entry| {
            let id = entry.key.parse::<i64>().ok()?;
            let snippet = snippets_by_id.get(&id)?;
            let evaluation = evaluate_snippet_status(snippet, &current_versions);
            Some(json!({
                "id": id,
                "title": snippet.title,
                "status": snippet.status,
                "effective_status": evaluation.effective_status,
                "stale_packages": stale_packages_json(&evaluation.stale_packages),
                "packages": snippet.packages,
                "tags": snippet.tags,
                "score": entry.score,
                "snippet": entry.snippet,
            }))
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "snippet_search",
        "payload": {
            "query": query,
            "filters": {
                "package": package,
                "tag": tag,
                "status": status,
            },
            "count": results.len(),
            "matches": results,
        }
    }))
}

fn snippet_delete_response(id: i64) -> Result<Value> {
    let mut store = IndexStore::open_default()?;
    let deleted = store.delete_snippet(id)?;
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "snippet_delete",
        "payload": {
            "id": id,
            "deleted": deleted,
        }
    }))
}

fn snippet_body_input(file: Option<&Path>, body: Option<&str>) -> Result<String> {
    let text = if let Some(file) = file {
        fs::read_to_string(file)
            .with_context(|| format!("failed to read snippet body from {}", file.display()))?
    } else if let Some(body) = body {
        body.to_string()
    } else {
        let mut stdin = String::new();
        std::io::stdin()
            .read_to_string(&mut stdin)
            .context("failed to read snippet body from stdin")?;
        stdin
    };

    let trimmed = text.trim();
    if trimmed.is_empty() {
        bail!("snippet body is empty");
    }
    Ok(trimmed.to_string())
}

fn indexed_package_versions(packages: &[String]) -> Result<BTreeMap<String, String>> {
    collect_current_package_versions(packages)
}

fn snippet_json(snippet: &IndexedSnippet, evaluation: &SnippetStatusEvaluation) -> Value {
    json!({
        "id": snippet.id,
        "title": snippet.title,
        "body": snippet.body,
        "packages": snippet.packages,
        "objects": snippet.objects,
        "tags": snippet.tags,
        "verbs": snippet.verbs,
        "status": snippet.status,
        "effective_status": evaluation.effective_status,
        "source": snippet.source,
        "package_versions": snippet.package_versions,
        "current_package_versions": evaluation.current_package_versions,
        "stale_packages": stale_packages_json(&evaluation.stale_packages),
        "created_at": snippet.created_at,
        "updated_at": snippet.updated_at,
    })
}

fn snippet_summary_json(snippet: &IndexedSnippet, evaluation: &SnippetStatusEvaluation) -> Value {
    json!({
        "id": snippet.id,
        "title": snippet.title,
        "status": snippet.status,
        "effective_status": evaluation.effective_status,
        "packages": snippet.packages,
        "tags": snippet.tags,
        "verbs": snippet.verbs,
        "stale_packages": stale_packages_json(&evaluation.stale_packages),
        "updated_at": snippet.updated_at,
        "body_preview": first_text_line(&snippet.body),
    })
}

fn first_text_line(text: &str) -> String {
    text.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default()
        .trim()
        .chars()
        .take(160)
        .collect()
}

fn current_package_versions_for_snippets(
    snippets: &[IndexedSnippet],
) -> Result<BTreeMap<String, String>> {
    let packages = snippets
        .iter()
        .flat_map(|snippet| snippet.packages.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    collect_current_package_versions(&packages)
}

fn collect_current_package_versions(packages: &[String]) -> Result<BTreeMap<String, String>> {
    let mut versions = BTreeMap::new();
    if packages.is_empty() {
        return Ok(versions);
    }

    let store = ensure_packages_indexed(packages)?;
    for package in packages {
        if let Some(summary) = store.get_indexed_package_summary(package)?
            && let Some(version) = summary.version
        {
            versions.insert(package.clone(), version);
        }
    }
    Ok(versions)
}

fn evaluate_snippet_status(
    snippet: &IndexedSnippet,
    current_versions: &BTreeMap<String, String>,
) -> SnippetStatusEvaluation {
    let mut stale_packages = Vec::new();
    let mut snapshot = BTreeMap::new();

    for package in &snippet.packages {
        let recorded_version = snippet.package_versions.get(package).cloned();
        let current_version = current_versions.get(package).cloned();
        if let Some(current_version) = &current_version {
            snapshot.insert(package.clone(), current_version.clone());
        }

        if recorded_version.is_some()
            && current_version.is_some()
            && recorded_version != current_version
        {
            stale_packages.push(StalePackageInfo {
                package: package.clone(),
                recorded_version,
                current_version,
            });
        }
    }

    let effective_status = if snippet.status == "failed" {
        "failed".to_string()
    } else if snippet.status == "stale" || !stale_packages.is_empty() {
        "stale".to_string()
    } else {
        snippet.status.clone()
    };

    SnippetStatusEvaluation {
        effective_status,
        stale_packages,
        current_package_versions: snapshot,
    }
}

fn stale_packages_json(stale_packages: &[StalePackageInfo]) -> Vec<Value> {
    stale_packages
        .iter()
        .map(|entry| {
            json!({
                "package": entry.package,
                "recorded_version": entry.recorded_version,
                "current_version": entry.current_version,
            })
        })
        .collect()
}

fn methods_across_response(generic: &str, packages: &[String]) -> Result<Value> {
    let store = if packages.is_empty() {
        IndexStore::open_default()?
    } else {
        ensure_packages_indexed(packages)?
    };
    let package_scope = if packages.is_empty() {
        store.indexed_packages()?
    } else {
        packages.to_vec()
    };
    let methods = store.find_methods(
        generic,
        if packages.is_empty() {
            None
        } else {
            Some(package_scope.as_slice())
        },
    )?;
    let matched_packages = methods
        .iter()
        .map(|method| method.package.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "methods_across",
        "payload": {
            "generic": generic,
            "packages": package_scope,
            "methods": methods,
            "counts": {
                "methods": methods.len(),
                "packages": matched_packages,
            }
        }
    }))
}

fn xref_response(package: &str, symbol: &str) -> Result<Value> {
    let packages = vec![package.to_string()];
    let store = ensure_packages_indexed(&packages)?;
    let package_data = store
        .get_indexed_package_data(package)?
        .ok_or_else(|| anyhow!("package '{package}' is not indexed"))?;
    let call_refs = store.get_call_refs(package)?;
    let outgoing_calls = call_refs
        .iter()
        .filter(|call_ref| call_ref.caller_symbol.as_deref() == Some(symbol))
        .map(call_ref_json)
        .collect::<Vec<_>>();
    let incoming_calls = store
        .find_calls_to_symbol(package, symbol)?
        .iter()
        .map(call_ref_json)
        .collect::<Vec<_>>();
    let local_mentions = collect_symbol_mentions(
        symbol,
        &store.get_indexed_files(package)?,
        &store.get_indexed_topics(package)?,
        &store.get_indexed_vignettes(package)?,
    );

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "xref",
        "payload": {
            "package": package,
            "symbol": symbol,
            "exported": package_data.exports.iter().any(|exported| exported == symbol),
            "defined_in_namespace": package_data.objects.iter().any(|object| object == symbol),
            "outgoing_calls": outgoing_calls,
            "incoming_calls": incoming_calls,
            "local_mentions": local_mentions,
            "counts": {
                "outgoing_calls": outgoing_calls.len(),
                "incoming_calls": incoming_calls.len(),
                "file_mentions": local_mentions["files"].as_array().map(Vec::len).unwrap_or_default(),
                "topic_mentions": local_mentions["topics"].as_array().map(Vec::len).unwrap_or_default(),
                "vignette_mentions": local_mentions["vignettes"].as_array().map(Vec::len).unwrap_or_default(),
            }
        }
    }))
}

fn used_by_response(package: &str, symbol: &str) -> Result<Value> {
    let packages = vec![package.to_string()];
    let store = ensure_packages_indexed(&packages)?;
    let callers = store
        .find_calls_to_symbol(package, symbol)?
        .iter()
        .map(call_ref_json)
        .collect::<Vec<_>>();
    let matched_packages = callers
        .iter()
        .filter_map(|call| call.get("package").and_then(Value::as_str))
        .collect::<std::collections::BTreeSet<_>>()
        .len();

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "used_by",
        "payload": {
            "package": package,
            "symbol": symbol,
            "callers": callers,
            "counts": {
                "calls": callers.len(),
                "packages": matched_packages,
            }
        }
    }))
}

fn bridge_response(package: &str, other_package: &str) -> Result<Value> {
    let package_list = vec![package.to_string(), other_package.to_string()];
    let store = ensure_packages_indexed(&package_list)?;

    let _left_data = store
        .get_indexed_package_data(package)?
        .ok_or_else(|| anyhow!("package '{package}' is not indexed"))?;
    let _right_data = store
        .get_indexed_package_data(other_package)?
        .ok_or_else(|| anyhow!("package '{other_package}' is not indexed"))?;
    let left_links = store.get_package_links(package)?;
    let right_links = store.get_package_links(other_package)?;
    let left_methods = store.get_indexed_methods(package)?;
    let right_methods = store.get_indexed_methods(other_package)?;
    let left_topics_data = store.get_indexed_topics(package)?;
    let right_topics_data = store.get_indexed_topics(other_package)?;
    let left_topics = left_topics_data
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let right_topics = right_topics_data
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let left_files = store.get_indexed_files(package)?;
    let right_files = store.get_indexed_files(other_package)?;
    let left_vignettes = store.get_indexed_vignettes(package)?;
    let right_vignettes = store.get_indexed_vignettes(other_package)?;
    let left_call_refs = store.get_call_refs(package)?;
    let right_call_refs = store.get_call_refs(other_package)?;

    let package_to_other = left_links
        .iter()
        .filter(|link| link.related_package == other_package)
        .map(|link| link.relation.clone())
        .collect::<Vec<_>>();
    let other_to_package = right_links
        .iter()
        .filter(|link| link.related_package == package)
        .map(|link| link.relation.clone())
        .collect::<Vec<_>>();

    let left_deps = left_links
        .iter()
        .map(|link| link.related_package.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let right_deps = right_links
        .iter()
        .map(|link| link.related_package.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let shared_dependencies = left_deps
        .intersection(&right_deps)
        .filter(|dependency| !is_bridge_noise_dependency(dependency))
        .take(20)
        .cloned()
        .collect::<Vec<_>>();

    let left_generics = left_methods
        .iter()
        .map(|method| method.generic.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let right_generics = right_methods
        .iter()
        .map(|method| method.generic.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let shared_generics = left_generics
        .intersection(&right_generics)
        .filter(|generic| !is_bridge_noise_generic(generic))
        .take(20)
        .cloned()
        .collect::<Vec<_>>();
    let shared_topics = left_topics
        .intersection(&right_topics)
        .take(20)
        .cloned()
        .collect::<Vec<_>>();

    let package_to_other_usage = directional_bridge_usage(
        package,
        other_package,
        &package_to_other,
        &left_call_refs,
        &left_files,
        &left_topics_data,
        &left_vignettes,
    );
    let other_to_package_usage = directional_bridge_usage(
        other_package,
        package,
        &other_to_package,
        &right_call_refs,
        &right_files,
        &right_topics_data,
        &right_vignettes,
    );

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "bridge",
        "payload": {
            "package": package,
            "other_package": other_package,
            "direct_relations": {
                "package_to_other": package_to_other,
                "other_to_package": other_to_package,
            },
            "direct_usage": {
                "package_to_other": package_to_other_usage,
                "other_to_package": other_to_package_usage,
            },
            "counts": {
                "package_methods": left_methods.len(),
                "other_package_methods": right_methods.len(),
                "shared_generics": shared_generics.len(),
                "shared_topics": shared_topics.len(),
            },
            "shared_generics": shared_generics,
            "shared_topics": shared_topics,
            "shared_dependencies": shared_dependencies,
            "drill_down": [
                format!("rpeek methods-across <generic> --package {package} --package {other_package}"),
                format!("rpeek map {package}"),
                format!("rpeek map {other_package}"),
            ],
        }
    }))
}

fn ensure_packages_indexed(packages: &[String]) -> Result<IndexStore> {
    let mut store = IndexStore::open_default()?;
    let mut helper: Option<HelperProcess> = None;
    let socket = env::temp_dir().join(format!(
        "rpeek-cross-index-{}-{}.sock",
        std::process::id(),
        now_timestamp().unwrap_or_default()
    ));

    let mut seen = std::collections::BTreeSet::new();
    for package in packages {
        if !seen.insert(package.clone()) {
            continue;
        }
        let package_name = package.as_str();
        let needs_index = match (
            store.get_package_state(package_name)?,
            store.get_indexed_package_summary(package_name)?,
        ) {
            (Some(state), Some(_)) if state.install_path.exists() => {
                package_fingerprint(&state.install_path)? != state.local_fingerprint
            }
            _ => true,
        };

        if !needs_index {
            continue;
        }

        if helper.is_none() {
            helper = Some(HelperProcess::start(&socket)?);
        }
        let helper_ref = helper
            .as_mut()
            .ok_or_else(|| anyhow!("failed to initialize indexing helper"))?;
        let record = build_indexed_package_record(package_name, helper_ref, &socket)?;
        let fingerprint = package_fingerprint(&record.install_path)?;
        let install_path = record.install_path.clone();
        let helper_fingerprint = record.version.clone();
        let indexed_at = record.indexed_at;
        store.upsert_package_record(&record)?;
        store.upsert_package_state(&PackageIndexState {
            package: package_name.to_string(),
            install_path,
            helper_fingerprint,
            local_fingerprint: fingerprint,
            updated_at: indexed_at,
        })?;
    }

    Ok(store)
}

fn is_bridge_noise_dependency(name: &str) -> bool {
    matches!(
        name,
        "covr"
            | "knitr"
            | "pkgdown"
            | "rmarkdown"
            | "testthat"
            | "roxygen2"
            | "devtools"
            | "usethis"
            | "withr"
    )
}

fn is_bridge_noise_generic(name: &str) -> bool {
    matches!(
        name,
        "as" | "print" | "plot" | "summary" | "show" | "format" | "c" | "names"
    )
}

fn namespace_imports_all(text: &str, target_package: &str) -> bool {
    text.lines().any(|line| {
        let compact = line.replace(' ', "");
        compact.contains(&format!("import({target_package})"))
    })
}

fn namespace_imported_symbols(text: &str, target_package: &str) -> Vec<String> {
    let mut symbols = std::collections::BTreeSet::new();
    let compact_target = target_package.replace(' ', "");
    for line in text.lines() {
        let compact = line.replace(' ', "");
        if let Some(rest) = compact.strip_prefix("importFrom(")
            && let Some(end) = rest.find(')')
        {
            let inner = &rest[..end];
            let mut parts = inner.split(',');
            if parts.next() == Some(compact_target.as_str()) {
                for symbol in parts {
                    if !symbol.is_empty() {
                        symbols.insert(symbol.to_string());
                    }
                }
            }
        }
        if let Some(rest) = line.trim().strip_prefix("@importFrom") {
            let mut parts = rest.split_whitespace();
            if parts.next() == Some(target_package) {
                for symbol in parts {
                    symbols.insert(symbol.trim_matches(',').to_string());
                }
            }
        }
    }
    symbols.into_iter().collect()
}

fn collect_package_mentions_in_files(files: &[IndexedFile], target_package: &str) -> Vec<Value> {
    let mut mentions = Vec::new();
    for file in files {
        if let Some(line_number) = first_line_with_package_name(&file.text, target_package) {
            mentions.push(json!({
                "path": file.path,
                "line": line_number,
            }));
        }
    }
    dedup_json_values(mentions)
}

fn collect_package_mentions_in_topics(topics: &[IndexedTopic], target_package: &str) -> Vec<Value> {
    let mut mentions = Vec::new();
    for topic in topics {
        if topic
            .text
            .as_deref()
            .is_some_and(|text| contains_package_name(text, target_package))
        {
            mentions.push(json!({
                "topic": topic.topic,
                "title": topic.title,
            }));
        }
    }
    dedup_json_values(mentions)
}

fn collect_package_mentions_in_vignettes(
    vignettes: &[IndexedVignette],
    target_package: &str,
) -> Vec<Value> {
    let mut mentions = Vec::new();
    for vignette in vignettes {
        if vignette
            .text
            .as_deref()
            .is_some_and(|text| contains_package_name(text, target_package))
        {
            mentions.push(json!({
                "topic": vignette.topic,
                "title": vignette.title,
            }));
        }
    }
    dedup_json_values(mentions)
}

fn first_line_with_package_name(text: &str, target_package: &str) -> Option<usize> {
    text.lines()
        .enumerate()
        .find(|(_, line)| contains_package_name(line, target_package))
        .map(|(index, _)| index + 1)
}

fn first_line_with_symbol(text: &str, symbol: &str) -> Option<usize> {
    text.lines()
        .enumerate()
        .find(|(_, line)| contains_symbol_name(line, symbol))
        .map(|(index, _)| index + 1)
}

fn contains_package_name(text: &str, target_package: &str) -> bool {
    let lower_text = text.to_ascii_lowercase();
    let lower_package = target_package.to_ascii_lowercase();
    lower_text.contains(&format!("{lower_package}::"))
        || lower_text.contains(&format!("{lower_package}:::"))
        || lower_text.contains(&lower_package)
}

fn contains_symbol_name(text: &str, symbol: &str) -> bool {
    text.contains(&format!("{symbol}(")) || text.contains(symbol)
}

fn dedup_json_values(values: Vec<Value>) -> Vec<Value> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        let key = value.to_string();
        if seen.insert(key) {
            out.push(value);
        }
    }
    out
}

fn directional_bridge_usage(
    from_package: &str,
    to_package: &str,
    dependency_relations: &[String],
    call_refs: &[rpeek::index::IndexedCallRef],
    files: &[IndexedFile],
    topics: &[IndexedTopic],
    vignettes: &[IndexedVignette],
) -> Value {
    const SAMPLE_LIMIT: usize = 20;

    let namespace = files
        .iter()
        .find(|file| file.path.eq_ignore_ascii_case("NAMESPACE"));
    let namespace_import_all = namespace
        .map(|file| namespace_imports_all(&file.text, to_package))
        .unwrap_or(false);
    let namespace_imports = namespace
        .map(|file| namespace_imported_symbols(&file.text, to_package))
        .unwrap_or_default();
    let symbol_refs = call_refs
        .iter()
        .filter(|call_ref| call_ref.callee_package.as_deref() == Some(to_package))
        .map(call_ref_json)
        .collect::<Vec<_>>();
    let file_mentions = collect_package_mentions_in_files(files, to_package);
    let topic_mentions = collect_package_mentions_in_topics(topics, to_package);
    let vignette_mentions = collect_package_mentions_in_vignettes(vignettes, to_package);

    json!({
        "from_package": from_package,
        "to_package": to_package,
        "dependency_relations": dependency_relations,
        "namespace_import_all": namespace_import_all,
        "namespace_imports": namespace_imports.into_iter().take(SAMPLE_LIMIT).collect::<Vec<_>>(),
        "symbol_refs": symbol_refs.into_iter().take(SAMPLE_LIMIT).collect::<Vec<_>>(),
        "mention_counts": {
            "files": file_mentions.len(),
            "topics": topic_mentions.len(),
            "vignettes": vignette_mentions.len(),
        },
        "file_mentions": file_mentions.into_iter().take(SAMPLE_LIMIT).collect::<Vec<_>>(),
        "topic_mentions": topic_mentions.into_iter().take(SAMPLE_LIMIT).collect::<Vec<_>>(),
        "vignette_mentions": vignette_mentions.into_iter().take(SAMPLE_LIMIT).collect::<Vec<_>>(),
    })
}

fn call_ref_json(call_ref: &rpeek::index::IndexedCallRef) -> Value {
    json!({
        "package": call_ref.package,
        "path": call_ref.file_path,
        "line": call_ref.line_number,
        "caller_symbol": call_ref.caller_symbol,
        "callee_package": call_ref.callee_package,
        "callee_symbol": call_ref.callee_symbol,
        "relation": call_ref.relation,
        "text": call_ref.snippet,
    })
}

fn collect_symbol_mentions(
    symbol: &str,
    files: &[IndexedFile],
    topics: &[IndexedTopic],
    vignettes: &[IndexedVignette],
) -> Value {
    json!({
        "files": files
            .iter()
            .filter_map(|file| first_line_with_symbol(&file.text, symbol).map(|line| {
                json!({
                    "path": file.path,
                    "line": line,
                })
            }))
            .take(20)
            .collect::<Vec<_>>(),
        "topics": topics
            .iter()
            .filter(|topic| {
                topic.topic == symbol
                    || topic.aliases.iter().any(|alias| alias == symbol)
                    || topic.text.as_deref().is_some_and(|text| contains_symbol_name(text, symbol))
            })
            .map(|topic| {
                json!({
                    "topic": topic.topic,
                    "title": topic.title,
                })
            })
            .take(20)
            .collect::<Vec<_>>(),
        "vignettes": vignettes
            .iter()
            .filter(|vignette| vignette.text.as_deref().is_some_and(|text| contains_symbol_name(text, symbol)))
            .map(|vignette| {
                json!({
                    "topic": vignette.topic,
                    "title": vignette.title,
                })
            })
            .take(20)
            .collect::<Vec<_>>(),
    })
}

fn build_indexed_package_record(
    package: &str,
    helper: &mut HelperProcess,
    socket: &Path,
) -> Result<IndexedPackageRecord> {
    let package_json = query_helper_payload(
        helper,
        socket,
        &Request::Pkg {
            package: package.to_string(),
        },
    )?;
    let install_path = PathBuf::from(required_string(&package_json, "install_path")?);
    let version = optional_string(&package_json, "version");
    let title = optional_string(&package_json, "title");
    let exports = string_list(&package_json, "exports");

    let objects_payload = query_helper_payload(
        helper,
        socket,
        &Request::Objects {
            package: package.to_string(),
        },
    )?;
    let objects = string_list(&objects_payload, "objects");

    let signatures_payload = query_helper_payload(
        helper,
        socket,
        &Request::Sigs {
            package: package.to_string(),
            all_objects: false,
        },
    )?;
    let signatures_json = signatures_payload
        .get("signatures")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));

    let topics_payload = query_helper_payload(
        helper,
        socket,
        &Request::Topics {
            package: package.to_string(),
        },
    )?;
    let topic_names = string_list(&topics_payload, "topics");
    let mut topics = Vec::new();
    for topic in topic_names {
        let doc_payload = query_helper_payload(
            helper,
            socket,
            &Request::Doc {
                package: package.to_string(),
                topic,
            },
        );
        if let Ok(doc_payload) = doc_payload {
            topics.push(indexed_topic_from_payload(&doc_payload));
        }
    }

    let vignette_payload = query_helper_payload(
        helper,
        socket,
        &Request::Vignettes {
            package: package.to_string(),
        },
    )?;
    let vignette_topics = vignette_payload
        .get("vignettes")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.get("topic").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut vignettes = Vec::new();
    for topic in vignette_topics {
        let payload = query_helper_payload(
            helper,
            socket,
            &Request::Vignette {
                package: package.to_string(),
                name: topic,
            },
        );
        if let Ok(payload) = payload {
            vignettes.push(indexed_vignette_from_payload(&payload));
        }
    }

    let files = collect_indexed_files(&install_path)?;

    Ok(IndexedPackageRecord {
        package: package.to_string(),
        version,
        title,
        install_path,
        package_json,
        exports,
        objects,
        signatures_json,
        topics,
        vignettes,
        files,
        indexed_at: now_timestamp()?,
    })
}

fn indexed_topic_from_payload(payload: &Value) -> IndexedTopic {
    IndexedTopic {
        topic: payload
            .get("topic")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        title: optional_string(payload, "title"),
        aliases: string_list(payload, "aliases"),
        description: optional_string(payload, "description"),
        usage: optional_string(payload, "usage"),
        value: optional_string(payload, "value"),
        examples: optional_string(payload, "examples"),
        text: optional_string(payload, "text"),
    }
}

fn indexed_vignette_from_payload(payload: &Value) -> IndexedVignette {
    let paths = payload.get("paths").cloned().unwrap_or(Value::Null);
    IndexedVignette {
        topic: payload
            .get("topic")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        title: optional_string(payload, "title"),
        source_path: optional_string(&paths, "source"),
        r_path: optional_string(&paths, "r"),
        pdf_path: optional_string(&paths, "pdf"),
        text_kind: optional_string(payload, "text_kind"),
        text: optional_string(payload, "text"),
    }
}

fn collect_indexed_files(install_path: &Path) -> Result<Vec<IndexedFile>> {
    let mut files = Vec::new();
    collect_indexed_files_from_dir(install_path, install_path, &mut files)?;
    Ok(files)
}

fn collect_indexed_files_from_dir(
    root: &Path,
    dir: &Path,
    out: &mut Vec<IndexedFile>,
) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read package directory {}", dir.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to scan package directory {}", dir.display()))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.is_dir() {
            collect_indexed_files_from_dir(root, &path, out)?;
            continue;
        }

        let rel_path = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if !should_index_file(&rel_path, &metadata) {
            continue;
        }
        if let Some(file) = read_indexed_file(&path, &rel_path)? {
            out.push(file);
        }
    }

    Ok(())
}

fn should_index_file(rel_path: &str, metadata: &fs::Metadata) -> bool {
    if metadata.len() > INDEX_TEXT_FILE_LIMIT {
        return false;
    }

    let rel_lower = rel_path.to_ascii_lowercase();
    let file_name = Path::new(rel_path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let extension = Path::new(rel_path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let top_level = rel_lower.split('/').next().unwrap_or_default();

    let top_level_allowed = matches!(
        top_level,
        "r" | "demo" | "doc" | "tests" | "testthat" | "inst"
    );
    let top_file_allowed = matches!(
        file_name.as_str(),
        "description"
            | "namespace"
            | "index"
            | "readme"
            | "readme.md"
            | "news"
            | "news.md"
            | "news.txt"
            | "changelog"
            | "changelog.md"
            | "citation"
            | "license"
            | "authors"
    );
    let extension_allowed = matches!(
        extension.as_str(),
        "r" | "rd"
            | "rmd"
            | "qmd"
            | "rnw"
            | "md"
            | "txt"
            | "html"
            | "htm"
            | "csv"
            | "tsv"
            | "json"
            | "yaml"
            | "yml"
            | "xml"
    );

    (top_level_allowed && extension_allowed) || top_file_allowed
}

fn read_indexed_file(path: &Path, rel_path: &str) -> Result<Option<IndexedFile>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.contains(&0) {
        return Ok(None);
    }

    let text = String::from_utf8_lossy(&bytes).trim().to_string();
    if text.is_empty() {
        return Ok(None);
    }

    let text_kind = Path::new(rel_path)
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("text")
        .to_ascii_lowercase();

    Ok(Some(IndexedFile {
        path: rel_path.to_string(),
        text_kind,
        text,
    }))
}

fn string_list(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn required_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("helper payload missing required field '{key}'"))
}

fn indexed_search_payload(
    package_data: &IndexedPackageData,
    topics: &[IndexedTopic],
    query: &str,
    kind: &str,
    limit: usize,
) -> Value {
    let limit = limit.max(1).min(100);
    let query_lower = query.to_ascii_lowercase();
    let exports = &package_data.exports;

    let object_matches = if matches!(kind, "all" | "object") {
        ranked_matches(
            &package_data.objects,
            &query_lower,
            limit,
            |name, matched_by| {
                json!({
                    "kind": "object",
                    "name": name,
                    "exported": exports.iter().any(|exported| exported == name),
                    "matched_by": matched_by,
                })
            },
        )
    } else {
        RankedMatches::empty()
    };

    let topic_names = topics
        .iter()
        .map(|topic| topic.topic.as_str())
        .collect::<Vec<_>>();
    let topic_matches = if matches!(kind, "all" | "topic") {
        ranked_matches(&topic_names, &query_lower, limit, |topic, matched_by| {
            json!({
                "kind": "topic",
                "topic": topic,
                "matched_by": matched_by,
            })
        })
    } else {
        RankedMatches::empty()
    };

    let mut matches = Vec::new();
    matches.extend(object_matches.matches);
    matches.extend(topic_matches.matches);
    matches.sort_by_cached_key(|entry| {
        let label = entry
            .get("name")
            .or_else(|| entry.get("topic"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        rank_tuple(&label, &query_lower)
    });
    matches.truncate(limit);

    json!({
        "package": package_data.package,
        "query": query,
        "kind": kind,
        "limit": limit,
        "matches": matches,
        "counts": {
            "objects": object_matches.total,
            "topics": topic_matches.total,
        }
    })
}

fn indexed_map_payload(
    package_data: &IndexedPackageData,
    topics: &[IndexedTopic],
    vignettes: &[IndexedVignette],
    files: &[IndexedFile],
) -> Value {
    const EXPORT_SAMPLE_LIMIT: usize = 24;
    const TOPIC_SAMPLE_LIMIT: usize = 20;
    const ENTRY_POINT_LIMIT: usize = 12;
    const FILE_SAMPLE_LIMIT: usize = 12;

    let dependencies = json!({
        "depends": string_list(&package_data.package_json, "depends"),
        "imports": string_list(&package_data.package_json, "imports"),
        "suggests": string_list(&package_data.package_json, "suggests"),
        "linking_to": string_list(&package_data.package_json, "linking_to"),
    });

    let startup_hooks = package_data
        .objects
        .iter()
        .filter(|name| {
            matches!(
                name.as_str(),
                ".onLoad" | ".onAttach" | ".onUnload" | ".onDetach"
            )
        })
        .cloned()
        .collect::<Vec<_>>();

    let entry_points = package_data
        .signatures_json
        .as_array()
        .into_iter()
        .flatten()
        .take(ENTRY_POINT_LIMIT)
        .filter_map(|entry| {
            Some(json!({
                "name": entry.get("name")?.as_str()?,
                "signature": entry.get("signature")?.as_str()?,
            }))
        })
        .collect::<Vec<_>>();

    let topic_samples = topics
        .iter()
        .take(TOPIC_SAMPLE_LIMIT)
        .map(|topic| {
            json!({
                "topic": topic.topic,
                "title": topic.title,
            })
        })
        .collect::<Vec<_>>();

    let file_samples = indexed_map_file_samples(package_data, files, FILE_SAMPLE_LIMIT);
    let exports_sample = package_data
        .exports
        .iter()
        .take(EXPORT_SAMPLE_LIMIT)
        .cloned()
        .collect::<Vec<_>>();

    json!({
        "package": package_data.package,
        "version": package_data.version,
        "title": optional_string(&package_data.package_json, "title"),
        "description": optional_string(&package_data.package_json, "description"),
        "install_path": package_data.install_path.display().to_string(),
        "counts": {
            "exports": package_data.exports.len(),
            "objects": package_data.objects.len(),
            "signatures": package_data.signatures_json.as_array().map(Vec::len).unwrap_or_default(),
            "topics": topics.len(),
            "vignettes": vignettes.len(),
        },
        "dependencies": dependencies,
        "startup_hooks": startup_hooks,
        "entry_points": entry_points,
        "exports_sample": exports_sample,
        "topic_samples": topic_samples,
        "vignettes": vignettes.iter().cloned().map(vignette_listing_json).collect::<Vec<_>>(),
        "file_samples": file_samples,
        "drill_down": [
            format!("rpeek search {} <query>", package_data.package),
            format!("rpeek sigs {}", package_data.package),
            format!("rpeek summary {} <object>", package_data.package),
            format!("rpeek index search {} <query>", package_data.package),
        ],
    })
}

fn indexed_search_vignettes_payload(
    package: &str,
    vignettes: &[IndexedVignette],
    query: &str,
    limit: usize,
) -> Value {
    let limit = limit.max(1).min(200);
    let mut matches = Vec::new();
    let mut scanned_files = 0usize;
    let mut truncated = false;

    for vignette in vignettes {
        if matches.len() >= limit {
            truncated = true;
            break;
        }

        if vignette_metadata_matches(vignette, query) {
            matches.push(json!({
                "topic": vignette.topic,
                "title": vignette.title,
                "matched_in": "metadata",
                "path": vignette.source_path.as_ref().or(vignette.r_path.as_ref()),
                "line": Value::Null,
                "text": Value::Null,
            }));
        }

        if matches.len() >= limit {
            truncated = true;
            break;
        }

        if let Some(text) = &vignette.text {
            scanned_files += 1;
            for (idx, line) in text.lines().enumerate() {
                if !contains_ignore_case(line, query) {
                    continue;
                }
                matches.push(json!({
                    "topic": vignette.topic,
                    "title": vignette.title,
                    "matched_in": "content",
                    "path": vignette.source_path.as_ref().or(vignette.r_path.as_ref()),
                    "line": idx + 1,
                    "text": line,
                }));
                if matches.len() >= limit {
                    truncated = true;
                    break;
                }
            }
        }
    }

    json!({
        "package": package,
        "query": query,
        "limit": limit,
        "counts": {
            "vignettes": vignettes.len(),
            "scanned_files": scanned_files,
        },
        "matches": matches,
        "truncated": truncated,
    })
}

fn vignette_listing_json(vignette: IndexedVignette) -> Value {
    json!({
        "topic": vignette.topic,
        "title": vignette.title,
        "file": basename_from_path(vignette.source_path.as_deref()),
        "r": basename_from_path(vignette.r_path.as_deref()),
        "pdf": basename_from_path(vignette.pdf_path.as_deref()),
        "paths": {
            "source": vignette.source_path,
            "r": vignette.r_path,
            "pdf": vignette.pdf_path,
        }
    })
}

fn vignette_detail_json(package: &str, vignette: &IndexedVignette) -> Value {
    json!({
        "package": package,
        "topic": vignette.topic,
        "title": vignette.title,
        "file": basename_from_path(vignette.source_path.as_deref()),
        "r": basename_from_path(vignette.r_path.as_deref()),
        "pdf": basename_from_path(vignette.pdf_path.as_deref()),
        "paths": {
            "source": vignette.source_path,
            "r": vignette.r_path,
            "pdf": vignette.pdf_path,
        },
        "text": vignette.text,
        "text_path": vignette.source_path.as_ref().or(vignette.r_path.as_ref()),
        "text_kind": vignette.text_kind,
    })
}

fn match_vignette(vignettes: &[IndexedVignette], name: &str) -> Option<IndexedVignette> {
    let lowered = name.to_ascii_lowercase();
    let exact = vignettes
        .iter()
        .find(|vignette| vignette.topic.eq_ignore_ascii_case(name))
        .cloned();
    if exact.is_some() {
        return exact;
    }

    let candidates = vignettes
        .iter()
        .filter(|vignette| {
            vignette
                .title
                .as_deref()
                .is_some_and(|title| title.eq_ignore_ascii_case(name))
                || basename_from_path(vignette.source_path.as_deref())
                    .as_deref()
                    .is_some_and(|file| file.eq_ignore_ascii_case(name))
        })
        .cloned()
        .collect::<Vec<_>>();
    if candidates.len() == 1 {
        return candidates.into_iter().next();
    }

    let contains = vignettes
        .iter()
        .filter(|vignette| {
            contains_ignore_case(&vignette.topic, &lowered)
                || vignette
                    .title
                    .as_deref()
                    .is_some_and(|title| contains_ignore_case(title, &lowered))
                || basename_from_path(vignette.source_path.as_deref())
                    .as_deref()
                    .is_some_and(|file| contains_ignore_case(file, &lowered))
        })
        .cloned()
        .collect::<Vec<_>>();
    if contains.len() == 1 {
        return contains.into_iter().next();
    }

    None
}

fn vignette_metadata_matches(vignette: &IndexedVignette, query: &str) -> bool {
    contains_ignore_case(&vignette.topic, query)
        || vignette
            .title
            .as_deref()
            .is_some_and(|title| contains_ignore_case(title, query))
        || vignette
            .source_path
            .as_deref()
            .is_some_and(|path| contains_ignore_case(path, query))
        || vignette
            .r_path
            .as_deref()
            .is_some_and(|path| contains_ignore_case(path, query))
}

fn basename_from_path(path: Option<&str>) -> Option<String> {
    path.and_then(|value| {
        Path::new(value)
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
    })
}

fn indexed_map_file_samples(
    package_data: &IndexedPackageData,
    files: &[IndexedFile],
    limit: usize,
) -> Vec<Value> {
    let install_path = package_data.install_path.display().to_string();
    let mut ranked = files.to_vec();
    ranked.sort_by_cached_key(|file| map_file_rank(&file.path));
    ranked
        .into_iter()
        .take(limit)
        .map(|file| {
            json!({
                "path": file.path,
                "kind": file.text_kind,
                "absolute_path": format!("{install_path}/{}", file.path),
            })
        })
        .collect()
}

fn map_file_rank(path: &str) -> (u8, u8, String) {
    let lower = path.to_ascii_lowercase();
    let group = if lower == "description" {
        0
    } else if lower == "namespace" {
        1
    } else if lower == "index" {
        2
    } else if lower.starts_with("doc/") {
        3
    } else if lower.starts_with("tests/") || lower.starts_with("testthat/") {
        4
    } else if lower.starts_with("r/") {
        5
    } else if lower.starts_with("inst/") {
        6
    } else {
        7
    };
    let depth = lower.matches('/').count() as u8;
    (group, depth, lower)
}

#[derive(Default)]
struct RankedMatches {
    matches: Vec<Value>,
    total: usize,
}

impl RankedMatches {
    fn empty() -> Self {
        Self::default()
    }
}

fn ranked_matches<F>(
    candidates: &[impl AsRef<str>],
    query_lower: &str,
    limit: usize,
    builder: F,
) -> RankedMatches
where
    F: Fn(&str, &str) -> Value,
{
    let labels = candidates
        .iter()
        .map(|candidate| candidate.as_ref().trim())
        .filter(|candidate| !candidate.is_empty())
        .collect::<Vec<_>>();
    if labels.is_empty() {
        return RankedMatches::empty();
    }

    let substring = labels
        .iter()
        .copied()
        .filter(|label| contains_ignore_case(label, query_lower))
        .collect::<Vec<_>>();
    let (pool, matched_by) = if substring.is_empty() {
        let mut ranked = labels;
        ranked.sort_by_cached_key(|label| rank_tuple(label, query_lower));
        (
            ranked
                .into_iter()
                .take(limit.saturating_mul(3).max(limit))
                .collect::<Vec<_>>(),
            "fuzzy",
        )
    } else {
        (substring, "substring")
    };

    let mut ranked = pool;
    ranked.sort_by_cached_key(|label| rank_tuple(label, query_lower));
    RankedMatches {
        total: ranked.len(),
        matches: ranked
            .into_iter()
            .take(limit)
            .map(|label| builder(label, matched_by))
            .collect(),
    }
}

fn rank_tuple(label: &str, query: &str) -> (u8, u8, u8, usize, usize, String) {
    let lowered = label.to_ascii_lowercase();
    (
        (!lowered.eq(query)) as u8,
        (!lowered.starts_with(query)) as u8,
        (!lowered.contains(query)) as u8,
        levenshtein(query, &lowered),
        label.len(),
        lowered,
    )
}

fn contains_ignore_case(text: &str, query: &str) -> bool {
    text.to_ascii_lowercase()
        .contains(&query.to_ascii_lowercase())
}

fn levenshtein(a: &str, b: &str) -> usize {
    if a == b {
        return 0;
    }
    if a.is_empty() {
        return b.chars().count();
    }
    if b.is_empty() {
        return a.chars().count();
    }

    let b_chars = b.chars().collect::<Vec<_>>();
    let mut prev = (0..=b_chars.len()).collect::<Vec<_>>();
    let mut curr = vec![0; b_chars.len() + 1];

    for (i, a_char) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, b_char) in b_chars.iter().enumerate() {
            let cost = usize::from(a_char != *b_char);
            curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b_chars.len()]
}

fn batch_response(file: Option<PathBuf>, options: &ResponseOptions) -> Result<Value> {
    let input = match file {
        Some(path) => fs::read_to_string(&path)
            .with_context(|| format!("failed to read batch file {}", path.display()))?,
        None => {
            let mut buffer = String::new();
            std::io::stdin().read_to_string(&mut buffer)?;
            buffer
        }
    };

    let mut responses = Vec::new();
    for (index, raw_line) in input.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        match serde_json::from_str::<Request>(line) {
            Ok(request) => match query_daemon(&request, options) {
                Ok(response) => responses.push(response),
                Err(err) => responses.push(batch_item_error(index, err.to_string())),
            },
            Err(err) => responses.push(batch_item_error(
                index,
                format!("invalid batch request JSON: {err}"),
            )),
        }
    }

    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "batch",
        "payload": {
            "responses": responses
        }
    }))
}

fn batch_item_error(index: usize, message: String) -> Value {
    json!({
        "schema_version": 1,
        "ok": false,
        "command": "batch_item",
        "batch_index": index,
        "error": {
            "code": "batch_request_error",
            "message": message,
        }
    })
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct CacheKey {
    request: Request,
    fingerprint: String,
}

#[derive(Clone, Debug)]
struct PackageState {
    install_path: PathBuf,
    fingerprint: String,
    helper_fingerprint: Option<String>,
}

enum FingerprintResolution {
    Fingerprint(String),
    ErrorResponse(String),
}

struct ResponseCache {
    entries: HashMap<CacheKey, String>,
    order: VecDeque<CacheKey>,
    packages: HashMap<String, PackageState>,
    index: IndexStore,
    hits: u64,
    misses: u64,
    invalidations: u64,
    max_entries: usize,
}

impl ResponseCache {
    fn new() -> Result<Self> {
        let max_entries = env::var("RPEEK_CACHE_ENTRIES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(512);
        Ok(Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            packages: HashMap::new(),
            index: IndexStore::open_default()?,
            hits: 0,
            misses: 0,
            invalidations: 0,
            max_entries,
        })
    }

    fn get(&mut self, key: &CacheKey) -> Option<String> {
        let response = self.entries.get(key).cloned();
        if response.is_some() {
            self.hits += 1;
            self.touch(key);
        } else {
            self.misses += 1;
        }
        response
    }

    fn insert(&mut self, key: CacheKey, response: String) {
        if self.entries.contains_key(&key) {
            self.entries.insert(key.clone(), response);
            self.touch(&key);
            return;
        }

        self.entries.insert(key.clone(), response);
        self.order.push_back(key);
        while self.entries.len() > self.max_entries {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn touch(&mut self, key: &CacheKey) {
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }

    fn clear_response(&mut self) -> String {
        let cleared_entries = self.entries.len();
        let cleared_packages = self.packages.len();
        self.entries.clear();
        self.order.clear();
        self.packages.clear();

        json!({
            "schema_version": 1,
            "ok": true,
            "payload": {
                "cleared_entries": cleared_entries,
                "cleared_packages": cleared_packages
            }
        })
        .to_string()
    }

    fn stats_payload(&self) -> Value {
        json!({
            "entries": self.entries.len(),
            "packages": self.packages.len(),
            "hits": self.hits,
            "misses": self.misses,
            "invalidations": self.invalidations,
            "max_entries": self.max_entries
        })
    }

    fn index_payload(&self) -> Value {
        match self.index.stats() {
            Ok(stats) => json!({
                "path": stats.path.display().to_string(),
                "schema_version": stats.schema_version,
                "packages": stats.package_count,
                "indexed_packages": stats.indexed_packages,
                "topics": stats.topic_count,
                "vignettes": stats.vignette_count,
                "files": stats.file_count
            }),
            Err(err) => json!({
                "error": err.to_string()
            }),
        }
    }

    fn stats_response(&self) -> String {
        json!({
            "schema_version": 1,
            "ok": true,
            "payload": self.stats_payload()
        })
        .to_string()
    }

    fn resolve_fingerprint(
        &mut self,
        package: &str,
        helper: &mut HelperProcess,
        socket: &Path,
    ) -> Result<FingerprintResolution> {
        if let Some(state) = self.packages.get(package) {
            if state.install_path.exists() {
                let latest = package_fingerprint(&state.install_path)?;
                if latest == state.fingerprint {
                    return Ok(FingerprintResolution::Fingerprint(
                        state.fingerprint.clone(),
                    ));
                }
            }

            let helper_fingerprint = state.helper_fingerprint.clone();
            self.invalidate_package(package);
            return self.refresh_package_state(package, helper_fingerprint, helper, socket);
        }

        if let Some(state) = self.index.get_package_state(package)? {
            if state.install_path.exists() {
                let latest = package_fingerprint(&state.install_path)?;
                if latest == state.local_fingerprint {
                    self.packages.insert(
                        package.to_string(),
                        PackageState {
                            install_path: state.install_path,
                            fingerprint: latest.clone(),
                            helper_fingerprint: state.helper_fingerprint,
                        },
                    );
                    return Ok(FingerprintResolution::Fingerprint(latest));
                }
            }

            self.invalidate_package(package);
            return self.refresh_package_state(package, state.helper_fingerprint, helper, socket);
        }

        self.refresh_package_state(package, None, helper, socket)
    }

    fn invalidate_package(&mut self, package: &str) {
        self.entries
            .retain(|key, _| key.request.package() != Some(package));
        self.order
            .retain(|key| key.request.package() != Some(package));
        self.packages.remove(package);
        self.invalidations += 1;
    }

    fn refresh_package_state(
        &mut self,
        package: &str,
        prior_helper_fingerprint: Option<String>,
        helper: &mut HelperProcess,
        socket: &Path,
    ) -> Result<FingerprintResolution> {
        let fingerprint_request = Request::Fingerprint {
            package: package.to_string(),
        };
        let fingerprint_line = serde_json::to_string(&fingerprint_request)?;
        let response = helper
            .send(&fingerprint_line)
            .or_else(|_| helper.restart(socket, &fingerprint_line))
            .context("failed to query R helper for package fingerprint")?;
        let value: Value =
            serde_json::from_str(&response).context("helper returned invalid JSON")?;
        if !response_reports_success(&value) {
            return Ok(FingerprintResolution::ErrorResponse(response));
        }
        let payload = value
            .get("payload")
            .ok_or_else(|| anyhow!("fingerprint response missing payload"))?;
        let install_path = payload
            .get("install_path")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("fingerprint response missing install_path"))?;
        let helper_fingerprint = payload
            .get("version")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(prior_helper_fingerprint);
        let install_path = PathBuf::from(install_path);
        let fingerprint = package_fingerprint(&install_path)?;
        let state = PackageState {
            install_path: install_path.clone(),
            fingerprint: fingerprint.clone(),
            helper_fingerprint: helper_fingerprint.clone(),
        };
        self.packages.insert(package.to_string(), state);
        self.index.upsert_package_state(&PackageIndexState {
            package: package.to_string(),
            install_path,
            helper_fingerprint,
            local_fingerprint: fingerprint.clone(),
            updated_at: now_timestamp()?,
        })?;

        Ok(FingerprintResolution::Fingerprint(fingerprint))
    }

    fn ensure_indexed_package(
        &mut self,
        package: &str,
        fingerprint: &str,
        helper: &mut HelperProcess,
        socket: &Path,
    ) -> Result<()> {
        let state = self.index.get_package_state(package)?;
        let summary = self.index.get_indexed_package_summary(package)?;
        if let (Some(state), Some(_)) = (state, summary)
            && state.local_fingerprint == fingerprint
        {
            return Ok(());
        }

        let record = build_indexed_package_record(package, helper, socket)?;
        let helper_fingerprint = record.version.clone();
        let install_path = record.install_path.clone();
        self.index.upsert_package_record(&record)?;
        self.index.upsert_package_state(&PackageIndexState {
            package: package.to_string(),
            install_path: install_path.clone(),
            helper_fingerprint: helper_fingerprint.clone(),
            local_fingerprint: fingerprint.to_string(),
            updated_at: record.indexed_at,
        })?;
        self.packages.insert(
            package.to_string(),
            PackageState {
                install_path,
                fingerprint: fingerprint.to_string(),
                helper_fingerprint,
            },
        );
        Ok(())
    }

    fn indexed_response(&self, request: &Request) -> Result<Option<String>> {
        indexed_request_response(self, request)
    }
}

fn package_fingerprint(install_path: &Path) -> Result<String> {
    let description = file_fingerprint(&install_path.join("DESCRIPTION"))?;
    let namespace = file_fingerprint(&install_path.join("NAMESPACE"))?;
    let meta = path_fingerprint(&install_path.join("Meta"))?;
    let help = path_fingerprint(&install_path.join("help"))?;
    let doc = path_fingerprint(&install_path.join("doc"))?;
    let r_dir = path_fingerprint(&install_path.join("R"))?;

    Ok(format!(
        "{}|description:{description}|namespace:{namespace}|meta:{meta}|help:{help}|doc:{doc}|r:{r_dir}",
        install_path.display()
    ))
}

fn index_status_response() -> Result<Value> {
    let store = IndexStore::open_default()?;
    let stats = store.stats()?;
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "index_status",
        "payload": {
            "path": stats.path.display().to_string(),
            "schema_version": stats.schema_version,
            "packages": stats.package_count,
            "indexed_packages": stats.indexed_packages,
            "topics": stats.topic_count,
            "vignettes": stats.vignette_count,
            "files": stats.file_count,
            "snippets": stats.snippet_count
        }
    }))
}

fn index_clear_response() -> Result<Value> {
    let store = IndexStore::open_default()?;
    let path = store.path().display().to_string();
    let cleared_packages = store.clear()?;
    Ok(json!({
        "schema_version": 1,
        "ok": true,
        "command": "index_clear",
        "payload": {
            "path": path,
            "cleared_packages": cleared_packages
        }
    }))
}

fn file_fingerprint(path: &Path) -> Result<String> {
    if !path.exists() {
        return Ok("missing".to_string());
    }

    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let modified = metadata
        .modified()
        .context("failed to read file modification time")?
        .duration_since(std::time::UNIX_EPOCH)
        .context("file modification time is before unix epoch")?
        .as_secs();

    Ok(format!("{}:{modified}", metadata.len()))
}

fn path_fingerprint(path: &Path) -> Result<String> {
    if !path.exists() {
        return Ok("missing".to_string());
    }

    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let modified = metadata
        .modified()
        .context("failed to read path modification time")?
        .duration_since(std::time::UNIX_EPOCH)
        .context("path modification time is before unix epoch")?
        .as_secs();

    Ok(modified.to_string())
}
