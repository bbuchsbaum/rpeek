# rpeek

Fast installed-R-package introspection for coding agents.

`rpeek` is a small Rust CLI for exploring installed R packages without writing throwaway `Rscript -e ...` probes. It is built for agent workflows: fast startup after the first call, JSON output by default, and commands that map directly to how an LLM explores code.

The installed binary is `rpeek`.

## Why This Exists

Installed R packages are awkward to inspect programmatically:

- raw source files are often unavailable
- docs live in help databases, not plain text
- object lookup, signatures, methods, and aliases require package-specific R code
- repeatedly starting `Rscript` is slow for iterative agent use

`rpeek` keeps a warm daemon alive behind the CLI and exposes the common introspection tasks directly.

The helper process requires the R package `jsonlite` for robust request/response encoding.

## Platform Support

`rpeek` currently targets Unix-like systems: macOS and Linux. It uses Unix domain sockets for the local daemon, so Windows is not supported yet unless run inside a Unix-like environment such as WSL.

## Install

You need three things:

- R
- Rust / Cargo
- the R package `jsonlite`

### 1. Prerequisites

| Prerequisite | How to get it |
|---|---|
| **R** | [cran.r-project.org](https://cran.r-project.org/) or `brew install r` on macOS |
| **Rust / cargo** | [rustup.rs](https://rustup.rs/) or your system package manager |
| **jsonlite** (R package) | `Rscript -e 'install.packages("jsonlite")'` |

After installing Rust, make sure Cargo's binary directory is on your `PATH`. For a default rustup install this is usually `~/.cargo/bin`.

### 2. Install rpeek

Install from GitHub:

```bash
cargo install --git https://github.com/bbuchsbaum/rpeek.git
```

Or from a local clone:

```bash
cargo install --path .
```

### 3. Verify the install

```bash
rpeek doctor
```

This checks that R is on your `PATH` and that `jsonlite` is installed. A healthy setup returns `"ok": true`.

Then try it out:

```bash
rpeek sig stats lm
rpeek doc stats lm
rpeek search-all lm
```

If R is installed but not named `R` on your `PATH`, point `rpeek` at it:

```bash
RPEEK_R_COMMAND=/full/path/to/R rpeek doctor
```

## What It Can Do

- package metadata and install location
- exported symbols
- full namespace object listing
- substring search across objects and help topics
- cross-package search across installed package exports and help topics
- search filtering by result kind and limit
- one-call object summaries
- function signatures
- best-effort source retrieval
- installed help / roxygen-derived docs
- S3 and S4 method discovery
- installed file listing
- daemon-local caching with stats and reset commands
- batch execution for multiple agent requests in one process

## Quick Start

If you installed with `cargo install`, run commands directly:

```bash
rpeek search stats lm
rpeek search-all --kind object --limit 10 lm
rpeek summary stats lm
rpeek source stats lm
rpeek doc stats lm
```

If you are working from a local clone, build first:

```bash
cargo build
```

Then run a few common commands:

```bash
cargo run -- search stats lm
cargo run -- search-all --kind object --limit 10 lm
cargo run -- summary stats lm
cargo run -- source stats lm
cargo run -- doc stats lm
cargo run -- exports stats
```

If you prefer the built binary:

```bash
target/debug/rpeek search stats lm
target/debug/rpeek summary stats lm
```

## Agent-Friendly Workflows

Find likely symbols when you only know part of a name:

```bash
cargo run -- search --kind object --limit 10 stats lm
```

Find a symbol when you do not know the package:

```bash
cargo run -- search-all --kind object --limit 10 lm
```

Get one compact summary payload:

```bash
cargo run -- summary stats lm
```

Read best-effort source:

```bash
cargo run -- source stats lm
```

Read installed docs:

```bash
cargo run -- doc stats lm
```

Run multiple requests in one process:

```bash
cat <<'EOF' | cargo run -- batch
{"action":"summary","package":"stats","name":"lm"}
{"action":"source","package":"stats","name":"lm"}
EOF
```

Get usage guidance from the tool itself:

```bash
cargo run -- agent
```

## Output Model

Output is JSON by default so agents can consume it directly.

Typical response shape:

```json
{
  "schema_version": 1,
  "command": "summary",
  "ok": true,
  "payload": {
    "...": "..."
  }
}
```

Errors are also structured and may include suggestions and a next-step hint. Request-level failures exit with status `2`; client/runtime failures exit with status `1`.

```json
{
  "schema_version": 1,
  "command": "sig",
  "ok": false,
  "error": {
    "code": "object_not_found",
    "message": "object 'lmx' not found in package 'stats'",
    "suggestions": ["lm", "glm", "nlm"],
    "hint": "Try `rpeek search stats lmx`."
  }
}
```

## Source Semantics

`source` returns one of:

- `raw_file`: a readable source-like file exists locally
- `deparsed`: source reconstructed from the installed object
- `unavailable`: no meaningful source can be recovered

For installed R packages, `deparsed` is often the expected case.

## Cache and Daemon Reuse

The CLI starts a background daemon on first use and caches successful responses in memory. By default, the socket path is derived from the current executable and temporary directory.

To force multiple calls to reuse the same warm daemon, set `RPEEK_SOCKET` explicitly:

```bash
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek cache clear
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek summary stats lm
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek cache stats
```

Useful cache commands:

```bash
target/debug/rpeek cache stats
target/debug/rpeek cache clear
```

Stop a daemon bound to an explicit socket:

```bash
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek shutdown
```

## Troubleshooting

If `rpeek doctor` reports that R is missing, install R or set `RPEEK_R_COMMAND` to the full path of the R binary.

If `jsonlite` is missing, install it in the R library used by the same R executable:

```bash
Rscript -e 'install.packages("jsonlite")'
```

If `rpeek` is not found after `cargo install`, add Cargo's binary directory to your shell path. With rustup, this is usually:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

## Development

Run the test suite:

```bash
cargo test
```

Useful manual checks:

```bash
cargo run -- search stats lm
cargo run -- search-all lm
cargo run -- search --kind topic --limit 5 stats lm
cargo run -- summary stats lm
cargo run -- source stats lm
cargo run -- doc stats lm
cargo run -- sig stats lmx
echo $?
```
