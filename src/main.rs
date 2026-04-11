use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use rpeek::protocol::Request;
use rpeek::response::{
    ResponseOptions, apply_response_options, response_exit_code, response_is_success,
    response_reports_success,
};
use rpeek::schema::{SchemaKind, schema_response};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
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
const HELPER_SCRIPT: &str = include_str!("r_helper.R");
const AFTER_HELP: &str = "\
Examples:
  rpeek search dplyr mutate
  rpeek search --kind topic --limit 5 stats lm
  rpeek search-all lm
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
    #[command(visible_alias = "src", about = "Best-effort source retrieval")]
    Source { package: String, name: String },
    #[command(about = "Installed help / roxygen-derived docs")]
    Doc { package: String, topic: String },
    #[command(about = "Related S3/S4 methods")]
    Methods { package: String, name: String },
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
        Commands::Doctor => Ok(doctor_response()),
        Commands::Schema { kind } => Ok(schema_response(kind)),
        Commands::Batch { file } => Ok(batch_response(file, &response_options)?),
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
        Commands::Source { package, name } => Request::Source { package, name },
        Commands::Doc { package, topic } => Request::Doc { package, topic },
        Commands::Methods { package, name } => Request::Methods { package, name },
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
        Commands::Agent => bail!("agent is not a daemon command"),
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
    let mut cache = ResponseCache::default();

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
            "cache": cache.stats_payload()
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
}

enum FingerprintResolution {
    Fingerprint(String),
    ErrorResponse(String),
}

struct ResponseCache {
    entries: HashMap<CacheKey, String>,
    order: VecDeque<CacheKey>,
    packages: HashMap<String, PackageState>,
    hits: u64,
    misses: u64,
    invalidations: u64,
    max_entries: usize,
}

impl Default for ResponseCache {
    fn default() -> Self {
        let max_entries = env::var("RPEEK_CACHE_ENTRIES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(512);
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            packages: HashMap::new(),
            hits: 0,
            misses: 0,
            invalidations: 0,
            max_entries,
        }
    }
}

impl ResponseCache {
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
        if let Some(state) = self.packages.get_mut(package) {
            let latest = package_fingerprint(&state.install_path)?;
            if latest != state.fingerprint {
                state.fingerprint = latest.clone();
                self.entries
                    .retain(|key, _| key.request.package() != Some(package));
                self.order
                    .retain(|key| key.request.package() != Some(package));
                self.invalidations += 1;
            }
            return Ok(FingerprintResolution::Fingerprint(
                state.fingerprint.clone(),
            ));
        }

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
        let install_path = value
            .get("payload")
            .and_then(|payload| payload.get("install_path"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("fingerprint response missing install_path"))?;
        let install_path = PathBuf::from(install_path);
        let fingerprint = package_fingerprint(&install_path)?;

        self.packages.insert(
            package.to_string(),
            PackageState {
                install_path,
                fingerprint: fingerprint.clone(),
            },
        );

        Ok(FingerprintResolution::Fingerprint(fingerprint))
    }
}

fn package_fingerprint(install_path: &Path) -> Result<String> {
    let description = file_fingerprint(&install_path.join("DESCRIPTION"))?;
    let namespace = file_fingerprint(&install_path.join("NAMESPACE"))?;
    let help = path_fingerprint(&install_path.join("help"))?;
    let r_dir = path_fingerprint(&install_path.join("R"))?;

    Ok(format!(
        "{}|description:{description}|namespace:{namespace}|help:{help}|r:{r_dir}",
        install_path.display()
    ))
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
