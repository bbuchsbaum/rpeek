# rpeek

Fast installed-R-package introspection for coding agents.

`rpeek` is a small Rust CLI for exploring installed R packages without writing throwaway `Rscript -e ...` probes. It is built for agent workflows: fast startup after the first call, JSON output by default, and commands that map directly to how an LLM explores code.

Current binary name: `rpkg`

That means local examples currently use `cargo run -- ...` or `target/debug/rpkg ...`. The repository can keep the `rpeek` project name even before the binary is renamed.

## Why This Exists

Installed R packages are awkward to inspect programmatically:

- raw source files are often unavailable
- docs live in help databases, not plain text
- object lookup, signatures, methods, and aliases require package-specific R code
- repeatedly starting `Rscript` is slow for iterative agent use

`rpeek` keeps a warm daemon alive behind the CLI and exposes the common introspection tasks directly.

## What It Can Do

- package metadata and install location
- exported symbols
- full namespace object listing
- substring search across objects and help topics
- one-call object summaries
- function signatures
- best-effort source retrieval
- installed help / roxygen-derived docs
- S3 and S4 method discovery
- installed file listing
- daemon-local caching with stats and reset commands

## Quick Start

Build:

```bash
cargo build
```

Run a few common commands:

```bash
cargo run -- search rMVPA feature
cargo run -- summary rMVPA feature_rsa_design
cargo run -- source rMVPA feature_rsa_design
cargo run -- doc rMVPA feature_rsa_design
cargo run -- exports rMVPA
```

If you prefer the built binary:

```bash
target/debug/rpkg search rMVPA feature
target/debug/rpkg summary rMVPA feature_rsa_design
```

## Agent-Friendly Workflows

Find likely symbols when you only know part of a name:

```bash
cargo run -- search stats lm
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

Errors are also structured and may include suggestions and a next-step hint:

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
RPKG_SOCKET=/tmp/rpeek-demo.sock target/debug/rpkg cache clear
RPKG_SOCKET=/tmp/rpeek-demo.sock target/debug/rpkg summary rMVPA feature_rsa_design
RPKG_SOCKET=/tmp/rpeek-demo.sock target/debug/rpkg cache stats
```

Useful cache commands:

```bash
target/debug/rpkg cache stats
target/debug/rpkg cache clear
```

## Development

Run the test suite:

```bash
cargo test
```

Useful manual checks:

```bash
cargo run -- search stats lm
cargo run -- summary stats lm
cargo run -- source stats lm
cargo run -- doc stats lm
cargo run -- sig stats lmx
```

## Current Status

The core CLI is working and validated against both tests and real installed packages. The current repo name is `rpeek`, while the crate/binary is still `rpkg`. Renaming the binary is straightforward, but the GitHub-facing documentation can already use the `rpeek` project name.
