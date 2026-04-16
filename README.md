# rpeek

Fast installed-R-package introspection for coding agents.

`rpeek` is a small Rust CLI for exploring installed R packages without writing throwaway `Rscript -e ...` probes. It is built for agent workflows: fast startup after the first call, JSON output by default, and commands that map directly to how an LLM explores code.

The installed binary is `rpeek`.

Rust API and internal architecture docs are published at <https://bbuchsbaum.github.io/rpeek/>.

## Why This Exists

Installed R packages are awkward to inspect programmatically:

- raw source files are often unavailable
- docs live in help databases, not plain text
- object lookup, signatures, methods, and aliases require package-specific R code
- repeatedly starting `Rscript` is slow for iterative agent use

`rpeek` keeps a warm daemon alive behind the CLI and exposes the common introspection tasks directly.

The helper process requires the R package `jsonlite` for robust request/response encoding.

## Agent Snippet

Paste this into an agent prompt when you want it to use `rpeek` effectively:

```text
Use `rpeek` to inspect installed R packages quickly.

- Start with `rpeek map <package>` for a one-shot package orientation pass. Use `rpeek pkg <package>` and `rpeek exports <package>` when you want the raw metadata and export list separately.
- If you only know part of a name, use `rpeek search <package> <query>` or `rpeek search-all <query>`.
- For many functions at once, use `rpeek sigs <package>`. Add `--all-objects` for non-exported namespace functions.
- For one object, use `rpeek summary <package> <name>` first, then drill into `rpeek sig`, `rpeek source`, `rpeek doc`, and `rpeek methods`.
- For cross-package work, use `rpeek methods-across <generic> --package <pkg>...` and `rpeek bridge <package> <other-package>`.
- For symbol-level tracing, use `rpeek xref <package> <symbol>` and `rpeek used-by <package> <symbol>`.
- Use `rpeek vignettes <package>`, `rpeek vignette <package> <name>`, and `rpeek search-vignettes <package> <query>` for installed vignette discovery.
- Use `rpeek grep <package> <query>` to search installed package files when docs or deparsed source are not enough.
- If you will query the same package repeatedly, pre-index it with `rpeek index package <package>`, then use `rpeek index show <package>` and `rpeek index search <package> <query>`.
- Store local workflow knowledge with `rpeek snippet add`, then retrieve it later with `rpeek snippet search` or `rpeek snippet list`.
- Output is JSON by default. Parse fields from the JSON instead of scraping prose.
- Use `--max-bytes` and `--no-examples` to keep payloads compact when needed.
```

## Platform Support

`rpeek` currently targets Unix-like systems: macOS and Linux. It uses Unix domain sockets for the local daemon, so Windows is not supported yet unless run inside a Unix-like environment such as WSL.

## Rust Docs

Browse the generated Rust docs locally:

```bash
cargo doc --no-deps --open
```

The GitHub Pages docs site is built from the same output and published by the `Docs` workflow.

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

### 2. Choose a user-local bin directory

For a new system, prefer installing `rpeek` into a user-owned bin directory that is already on your shell `PATH`, or that you can add once in your shell startup file.

Recommended default on macOS and Linux:

```bash
mkdir -p "$HOME/.local/bin"
export PATH="$HOME/.local/bin:$PATH"
```

If you prefer `~/bin` instead:

```bash
mkdir -p "$HOME/bin"
export PATH="$HOME/bin:$PATH"
```

To make that persistent, add the matching line to your shell startup file such as `~/.zshrc`, `~/.bashrc`, or `~/.profile`.

### 3. Install rpeek

Install from GitHub into `~/.local/bin`:

```bash
cargo install --git https://github.com/bbuchsbaum/rpeek.git --root "$HOME/.local"
```

Install from GitHub into `~/bin`:

```bash
cargo install --git https://github.com/bbuchsbaum/rpeek.git --root "$HOME"
```

Or from a local clone:

```bash
cargo install --path . --root "$HOME/.local"
```

### 4. Verify the install

```bash
rpeek doctor
```

This checks that R is on your `PATH` and that `jsonlite` is installed. A healthy setup returns `"ok": true`.

Then try it out:

```bash
rpeek sig stats lm
rpeek sigs stats
rpeek doc stats lm
rpeek search-all lm
```

If `rpeek` is not found after install, check that your chosen bin directory is on `PATH`:

```bash
command -v rpeek
echo "$PATH"
```

If R is installed but not named `R` on your `PATH`, point `rpeek` at it:

```bash
RPEEK_R_COMMAND=/full/path/to/R rpeek doctor
```

## What It Can Do

- package metadata and install location
- one-shot package orientation map for agents
- exported symbols
- full namespace object listing
- substring search across objects and help topics
- cross-package search across installed package exports and help topics
- search filtering by result kind and limit
- one-call object summaries
- function signatures
- package-wide function signature listing
- best-effort source retrieval
- installed help / roxygen-derived docs
- installed vignette listing, reading, and search
- S3 and S4 method discovery
- cross-package method lookup across indexed packages
- package-to-package dependency and overlap summaries
- best-effort symbol xref and caller lookup from indexed files
- installed file listing
- daemon-local caching with stats and reset commands
- persistent index storage for package metadata, docs, vignettes, examples, and file text
- indexed workflow snippets with status and package-version metadata
- lazy index-backed routing for fast package metadata and package-scoped search commands
- bm25-ranked FTS search for indexed package docs and snippets
- manual warm-path benchmark harness with latency targets
- batch execution for multiple agent requests in one process

## Quick Start

If you installed with `cargo install`, run commands directly:

```bash
rpeek search stats lm
rpeek map stats
rpeek methods-across plot --package stats --package graphics
rpeek bridge stats graphics
rpeek xref stats lm
rpeek used-by graphics plot
rpeek snippet add --title "Read BIDS preproc scan" --package bidser --package neuroim2 --tag workflow --body "Use bidser to locate scans, then read them with neuroim2."
rpeek snippet edit 1 --title "Read derivative BIDS scan" --tag bids --body "Use bidser to find a derivative scan, then load it with neuroim2."
rpeek snippet export --all --file snippets.json
rpeek snippet import --file snippets.json
rpeek snippet search "bids workflow"
rpeek snippet refresh 1 --status verified
rpeek search-all --kind object --limit 10 lm
rpeek sigs stats
rpeek vignettes stats
rpeek search-vignettes stats reshape
rpeek resolve lm
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
cargo run -- map stats
cargo run -- methods-across plot --package stats --package graphics
cargo run -- bridge stats graphics
cargo run -- xref stats lm
cargo run -- used-by graphics plot
cargo run -- snippet add --title "Read BIDS preproc scan" --package bidser --package neuroim2 --tag workflow --body "Use bidser to locate scans, then read them with neuroim2."
cargo run -- snippet edit 1 --title "Read derivative BIDS scan" --tag bids --body "Use bidser to find a derivative scan, then load it with neuroim2."
cargo run -- snippet export --all --file snippets.json
cargo run -- snippet import --file snippets.json
cargo run -- snippet search "bids workflow"
cargo run -- snippet refresh 1 --status verified
cargo run -- search-all --kind object --limit 10 lm
cargo run -- sigs stats
cargo run -- vignettes stats
cargo run -- search-vignettes stats reshape
cargo run -- resolve lm
cargo run -- summary stats lm
cargo run -- source stats lm
cargo run -- doc stats lm
cargo run -- exports stats
```

If you prefer the built binary:

```bash
target/debug/rpeek search stats lm
target/debug/rpeek map stats
target/debug/rpeek sigs stats
target/debug/rpeek vignettes stats
target/debug/rpeek summary stats lm
target/debug/rpeek snippet list
```

## Agent-Friendly Workflows

Find likely symbols when you only know part of a name:

```bash
cargo run -- search --kind object --limit 10 stats lm
```

Get a one-shot package map before drilling down:

```bash
cargo run -- map stats
```

Find a symbol when you do not know the package:

```bash
cargo run -- search-all --kind object --limit 10 lm
```

Resolve likely objects and topics from a query:

```bash
cargo run -- resolve lm
```

Get one compact summary payload:

```bash
cargo run -- summary stats lm
```

List exported function signatures for one package:

```bash
cargo run -- sigs stats
```

Include non-exported namespace functions too:

```bash
cargo run -- sigs --all-objects stats
```

List installed vignettes:

```bash
cargo run -- vignettes stats
```

Read one installed vignette:

```bash
cargo run -- vignette stats reshape
```

Search installed vignette titles and text:

```bash
cargo run -- search-vignettes stats reshape
```

Read best-effort source:

```bash
cargo run -- source stats lm
```

Read installed docs:

```bash
cargo run -- doc stats lm
```

Keep large responses compact:

```bash
cargo run -- --max-bytes 4000 --no-examples doc stats lm
```

Search installed package files:

```bash
cargo run -- grep stats lm.fit
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

Inspect, build, search, or clear the persistent index store:

```bash
target/debug/rpeek index status
target/debug/rpeek index package stats
target/debug/rpeek index show stats
target/debug/rpeek index search stats reshape
target/debug/rpeek index search stats '"predict" OR lm' --raw-match
target/debug/rpeek index clear
target/debug/rpeek snippet add --title "Read BIDS preproc scan" --package bidser --package neuroim2 --tag workflow --status verified --body "Use bidser to locate scans, then read them with neuroim2."
target/debug/rpeek snippet edit 1 --title "Read derivative BIDS scan" --tag bids --body "Use bidser to find a derivative scan, then load it with neuroim2."
target/debug/rpeek snippet export --all --file snippets.json
target/debug/rpeek snippet import --file snippets.json
target/debug/rpeek snippet search "bids workflow"
target/debug/rpeek snippet search '"predict" OR lm' --raw-match
target/debug/rpeek snippet list --package bidser
target/debug/rpeek snippet refresh 1 --status verified
```

Inspect, stop, or restart a daemon bound to an explicit socket:

```bash
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek daemon status
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek daemon stop
RPEEK_SOCKET=/tmp/rpeek-demo.sock target/debug/rpeek daemon restart
```

For a single isolated request without daemon reuse:

```bash
target/debug/rpeek --no-daemon summary stats lm
```

Cache size defaults to 512 successful responses. Override it with `RPEEK_CACHE_ENTRIES`.

The persistent index path defaults to `~/.cache/rpeek/index.sqlite3` (or `XDG_CACHE_HOME` when set). Override it with `RPEEK_INDEX_PATH`.

`index package` stores one package bundle in SQLite, including package metadata, exported signatures, help topics and examples, installed vignettes, and selected text files from the installed package tree. `index search` queries that stored bundle without round-tripping through R for each search, and now uses SQLite `bm25(...)` ranking with a title boost so obvious topic hits rise above long file matches.

Package-scoped metadata and search commands such as `pkg`, `exports`, `objects`, `search`, `sigs`, `vignettes`, `vignette`, and `search-vignettes` will lazily build or refresh that package index on first access when you are using the daemon-backed CLI path.

`snippet add`, `snippet list`, `snippet show`, `snippet edit`, `snippet export`, `snippet import`, `snippet search`, `snippet refresh`, and `snippet delete` use the same SQLite store for local workflow notes. Snippets keep package names, tags, verbs, a status field (`unknown`, `verified`, `stale`, `failed`), and the package versions known at insert time.

When you retrieve snippets, `rpeek` now reports both the stored `status` and an `effective_status`. If a referenced package version in the index no longer matches the version captured when the snippet was added, `effective_status` becomes `stale` and the payload includes `stale_packages` with recorded vs current versions.

Use `snippet edit <id>` for in-place updates to title, body, tags, objects, verbs, or package associations. Edits also rewrite the snippet’s FTS search row and recompute recorded package versions for the final package set.

Use `snippet export` and `snippet import` to move workflow notes between machines. Export bundles now carry a stable snippet key, and import will merge on that key instead of blindly inserting duplicates. For older bundles without keys, import falls back to a content fingerprint. The JSON bundle keeps the original recorded package-version snapshot, so an imported snippet can still show up as `effective_status: "stale"` if the target machine has different package versions.

Use `snippet refresh <id>` to rewrite the recorded package versions from the current index. Add `--status verified` when you want the stored status reset after you have re-checked the workflow.

Indexed FTS search now normalizes punctuation-heavy queries before they reach SQLite `MATCH`. Inputs like `pkg::fn`, `predict.lm`, or `cross-machine` are tokenized into safe lexical queries instead of relying on raw FTS syntax.

For debugging, indexed search payloads now include `match_query`, the exact expression sent to SQLite. If you really do want native FTS syntax, use `--raw-match` with `index search` or `snippet search`.

## Performance

Run the warm-path benchmark harness in release mode:

```bash
cargo test --release --test perf -- --ignored --nocapture
```

Representative warm-path targets:

- `map stats`: median <= 300ms, p95 <= 500ms
- `search --kind topic --limit 5 stats lm`: median <= 150ms, p95 <= 300ms
- `bridge stats graphics`: median <= 300ms, p95 <= 550ms

To fail the benchmark run when targets are missed:

```bash
RPEEK_ENFORCE_BENCH_TARGETS=1 cargo test --release --test perf -- --ignored --nocapture
```

## Protocol Schema

Print the current JSON request or response contract:

```bash
rpeek schema request
rpeek schema response
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
cargo run -- vignettes stats
cargo run -- vignette stats reshape
cargo run -- search-vignettes stats reshape
cargo run -- sig stats lmx
cargo run -- sigs stats
cargo run -- resolve lm
cargo run -- grep stats lm
echo $?
```
