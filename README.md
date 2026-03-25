# rpeek

Fast installed-R-package introspection for coding agents.

`rpeek` is a small Rust CLI for exploring installed R packages without writing throwaway `Rscript -e ...` probes. It is built for agent workflows: fast startup after the first call, JSON output by default, and commands that map directly to how an LLM explores code.

Current binary name: `rpeek`

## Why This Exists

Installed R packages are awkward to inspect programmatically:

- raw source files are often unavailable
- docs live in help databases, not plain text
- object lookup, signatures, methods, and aliases require package-specific R code
- repeatedly starting `Rscript` is slow for iterative agent use

`rpeek` keeps a warm daemon alive behind the CLI and exposes the common introspection tasks directly.

The helper process requires the R package `jsonlite` for robust request/response encoding.

## Install

You need three things: **R**, the **Rust toolchain** (`cargo`), and the R package **jsonlite**.

### 1. Prerequisites

| Prerequisite | How to get it |
|---|---|
| **R** | [cran.r-project.org](https://cran.r-project.org/) or `brew install r` on macOS |
| **Rust / cargo** | [rustup.rs](https://rustup.rs/) — one-liner: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| **jsonlite** (R package) | `Rscript -e 'install.packages("jsonlite")'` |

### 2. Install rpeek

From GitHub (recommended):

```bash
cargo install --git https://github.com/bbuchsbaum/rpeek.git
```

Or from a local clone:

```bash
cargo install --path .
```

### 3. Verify

```bash
rpeek doctor
```

This checks that R is on your PATH and jsonlite is installed. You should see `"ok": true` for every check. If something is missing, the output tells you exactly what to run.

Then try it out:

```bash
rpeek search-all lm
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
rpeek search rMVPA feature
rpeek search-all feature_rsa_design
rpeek summary rMVPA feature_rsa_design
```

If you are working from a local clone, build first:

```bash
cargo build
```

Then run a few common commands:

```bash
cargo run -- search rMVPA feature
cargo run -- search-all feature_rsa_design
cargo run -- summary rMVPA feature_rsa_design
cargo run -- source rMVPA feature_rsa_design
cargo run -- doc rMVPA feature_rsa_design
cargo run -- exports rMVPA
```

If you prefer the built binary:

```bash
target/debug/rpeek search rMVPA feature
target/debug/rpeek summary rMVPA feature_rsa_design
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

The CLI uses a background daemon and caches successful responses in memory. To force multiple calls to reuse the same warm daemon, set a socket path explicitly:

```bash
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek cache clear
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek summary rMVPA feature_rsa_design
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek cache stats
```

Useful cache commands:

```bash
target/debug/rpeek cache stats
target/debug/rpeek cache clear
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
