use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const SOCKET_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTHCHECK_TIMEOUT: Duration = Duration::from_millis(500);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const HELPER_SCRIPT: &str = include_str!("r_helper.R");
const AFTER_HELP: &str = "\
Examples:
  rpkg search dplyr mutate
  rpkg summary dplyr mutate
  rpkg source dplyr mutate
  rpkg doc dplyr mutate
  rpkg exports dplyr
  rpkg agent";

#[derive(Debug, Parser)]
#[command(
    name = "rpkg",
    version,
    about = "Fast installed-R-package introspection for coding agents",
    after_help = AFTER_HELP
)]
struct Cli {
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
    Search { package: String, query: String },
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
    #[command(about = "Quick usage guide for agents and scripts")]
    Agent,
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

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
struct Request {
    action: String,
    package: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
}

fn main() {
    let exit_code = match run() {
        Ok(value) => {
            println!("{value}");
            0
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
        command => {
            let request = request_from_command(command)?;
            query_daemon(&request)
        }
    }
}

fn request_from_command(command: Commands) -> Result<Request> {
    let request = match command {
        Commands::Pkg { package } => Request {
            action: "pkg".to_string(),
            package,
            name: None,
            query: None,
            topic: None,
        },
        Commands::Exports { package } => Request {
            action: "exports".to_string(),
            package,
            name: None,
            query: None,
            topic: None,
        },
        Commands::Objects { package } => Request {
            action: "objects".to_string(),
            package,
            name: None,
            query: None,
            topic: None,
        },
        Commands::Search { package, query } => Request {
            action: "search".to_string(),
            package,
            name: None,
            query: Some(query),
            topic: None,
        },
        Commands::Summary { package, name } => Request {
            action: "summary".to_string(),
            package,
            name: Some(name),
            query: None,
            topic: None,
        },
        Commands::Sig { package, name } => Request {
            action: "sig".to_string(),
            package,
            name: Some(name),
            query: None,
            topic: None,
        },
        Commands::Source { package, name } => Request {
            action: "source".to_string(),
            package,
            name: Some(name),
            query: None,
            topic: None,
        },
        Commands::Doc { package, topic } => Request {
            action: "doc".to_string(),
            package,
            name: None,
            query: None,
            topic: Some(topic),
        },
        Commands::Methods { package, name } => Request {
            action: "methods".to_string(),
            package,
            name: Some(name),
            query: None,
            topic: None,
        },
        Commands::Files { package } => Request {
            action: "files".to_string(),
            package,
            name: None,
            query: None,
            topic: None,
        },
        Commands::Cache { command } => match command {
            CacheCommands::Clear => Request {
                action: "cache_clear".to_string(),
                package: String::new(),
                name: None,
                query: None,
                topic: None,
            },
            CacheCommands::Stats => Request {
                action: "cache_stats".to_string(),
                package: String::new(),
                name: None,
                query: None,
                topic: None,
            },
        },
        Commands::Agent => bail!("agent is not a daemon command"),
        Commands::Serve { .. } => bail!("serve is not a client command"),
    };

    Ok(request)
}

fn query_daemon(request: &Request) -> Result<Value> {
    let socket = socket_path();
    ensure_daemon_running(&socket)?;

    let line = serde_json::to_string(request)?;
    let response = send_request_with_retry(&socket, &line)?;
    let response = response.trim();

    let mut value: Value = serde_json::from_str(response)
        .with_context(|| format!("invalid JSON response from daemon: {response}"))?;
    if let Some(map) = value.as_object_mut() {
        map.insert("command".to_string(), Value::String(request.action.clone()));
    }
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
        command.spawn().context("failed to spawn rpkg daemon")?;
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

    let ping = serde_json::to_string(&Request {
        action: "ping".to_string(),
        package: String::new(),
        name: None,
        query: None,
        topic: None,
    });
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
            if socket.exists() {
                let _ = fs::remove_file(socket);
            }
            ensure_daemon_running(socket)?;
            send_request_line(socket, line, REQUEST_TIMEOUT)
        }
    }
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
                let response = match read_request_line(&mut stream).and_then(|line| {
                    let request: Request =
                        serde_json::from_str(&line).context("failed to parse client request")?;
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
            }
            Err(err) => return Err(err).context("failed to accept daemon connection"),
        }
    }

    Ok(())
}

fn handle_request(
    request: &Request,
    line: &str,
    helper: &mut HelperProcess,
    cache: &mut ResponseCache,
    socket: &Path,
) -> Result<String> {
    match request.action.as_str() {
        "ping" => Ok(json!({
            "schema_version": 1,
            "ok": true,
            "payload": { "status": "ok" }
        })
        .to_string()),
        "cache_clear" => Ok(cache.clear_response()),
        "cache_stats" => Ok(cache.stats_response()),
        _ => handle_query_request(request, line, helper, cache, socket),
    }
}

fn handle_query_request(
    request: &Request,
    line: &str,
    helper: &mut HelperProcess,
    cache: &mut ResponseCache,
    socket: &Path,
) -> Result<String> {
    if request.package.is_empty() {
        bail!("request is missing package");
    }

    if request.is_cacheable() {
        let fingerprint = cache.resolve_fingerprint(&request.package, helper, socket)?;
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
            .stderr(Stdio::null())
            .spawn()
            .context("failed to start R helper process")?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open stdin for R helper"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to open stdout for R helper"))?;

        let mut helper = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            script_path,
        };
        let ping = serde_json::to_string(&Request {
            action: "ping".to_string(),
            package: String::new(),
            name: None,
            query: None,
            topic: None,
        })?;
        helper.send(&ping).context("R helper failed startup ping")?;
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
            bail!("R helper exited before replying");
        }

        Ok(response.trim().to_string())
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
    env::temp_dir().join(format!("rpkg-{user}-{build_tag}.sock"))
}

fn helper_script_path(socket: &Path) -> PathBuf {
    let stem = socket
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("rpkg");
    socket.with_file_name(format!("{stem}-helper.R"))
}

fn socket_lock_path(socket: &Path) -> PathBuf {
    let stem = socket
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("rpkg.sock");
    socket.with_file_name(format!("{stem}.lock"))
}

fn r_command() -> String {
    env::var("RPKG_R_COMMAND").unwrap_or_else(|_| "R".to_string())
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
                    "command": "rpkg search <package> <query>"
                },
                {
                    "task": "Get one-call object summary",
                    "command": "rpkg summary <package> <object>"
                },
                {
                    "task": "Read source",
                    "command": "rpkg source <package> <object>"
                },
                {
                    "task": "Read docs",
                    "command": "rpkg doc <package> <topic>"
                },
                {
                    "task": "List exports",
                    "command": "rpkg exports <package>"
                }
            ],
            "notes": [
                "JSON is the default output format.",
                "Use RPKG_SOCKET=/tmp/<name>.sock to reuse one warm daemon across calls.",
                "Source kind can be raw_file, deparsed, or unavailable."
            ]
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

#[derive(Debug, Default)]
struct ResponseCache {
    entries: HashMap<CacheKey, String>,
    packages: HashMap<String, PackageState>,
    hits: u64,
    misses: u64,
    invalidations: u64,
}

impl ResponseCache {
    fn get(&mut self, key: &CacheKey) -> Option<String> {
        let response = self.entries.get(key).cloned();
        if response.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        response
    }

    fn insert(&mut self, key: CacheKey, response: String) {
        self.entries.insert(key, response);
    }

    fn clear_response(&mut self) -> String {
        let cleared_entries = self.entries.len();
        let cleared_packages = self.packages.len();
        self.entries.clear();
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

    fn stats_response(&self) -> String {
        json!({
            "schema_version": 1,
            "ok": true,
            "payload": {
                "entries": self.entries.len(),
                "packages": self.packages.len(),
                "hits": self.hits,
                "misses": self.misses,
                "invalidations": self.invalidations
            }
        })
        .to_string()
    }

    fn resolve_fingerprint(
        &mut self,
        package: &str,
        helper: &mut HelperProcess,
        socket: &Path,
    ) -> Result<String> {
        if let Some(state) = self.packages.get_mut(package) {
            let latest = package_fingerprint(&state.install_path)?;
            if latest != state.fingerprint {
                state.fingerprint = latest.clone();
                self.entries.retain(|key, _| key.request.package != package);
                self.invalidations += 1;
            }
            return Ok(state.fingerprint.clone());
        }

        let fingerprint_request = Request {
            action: "fingerprint".to_string(),
            package: package.to_string(),
            name: None,
            query: None,
            topic: None,
        };
        let fingerprint_line = serde_json::to_string(&fingerprint_request)?;
        let response = helper
            .send(&fingerprint_line)
            .or_else(|_| helper.restart(socket, &fingerprint_line))
            .context("failed to query R helper for package fingerprint")?;
        let value: Value =
            serde_json::from_str(&response).context("helper returned invalid JSON")?;
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

        Ok(fingerprint)
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

fn response_is_success(response: &str) -> bool {
    serde_json::from_str::<Value>(response)
        .ok()
        .and_then(|value| value.get("ok").and_then(Value::as_bool))
        .unwrap_or(false)
}

impl Request {
    fn is_cacheable(&self) -> bool {
        !matches!(
            self.action.as_str(),
            "ping" | "cache_clear" | "cache_stats" | "fingerprint"
        )
    }
}
