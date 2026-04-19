---
name: rpeek
description: Probe, discover, and search installed R packages with the `rpeek` CLI. Use whenever you need to inspect an installed R package - find exported symbols, read function signatures, get help docs, locate methods for a generic, search across packages, read vignettes, or trace how a symbol is used. Prefer this over ad-hoc `Rscript -e ...` probes whenever the question is about an installed package.
---

# rpeek

`rpeek` is a fast CLI for inspecting installed R packages. It keeps a warm
helper process alive between calls and emits structured JSON, so it is the
right tool whenever you need to understand what an R package contains, how a
function is shaped, where a method lives, or what an installed vignette says.

## When to use this skill

Use `rpeek` whenever any of the following are true:

- You need to find a function, S3/S4 method, dataset, or help topic in an
  installed R package and you only have part of the name.
- You want to read a function signature, source, or help text without writing
  a throwaway `Rscript -e '...'` snippet.
- You need to enumerate exported (or all namespace) functions for a package.
- You need to know which package owns a given method for a generic, or how
  two packages relate.
- You want to search installed vignettes or installed package files.
- You will hit the same package repeatedly and want index-backed search.

If the user is asking about a CRAN page, source on GitHub, or an uninstalled
package, `rpeek` is not the right tool - it only sees what is installed
locally.

## Verifying the install

Before relying on `rpeek`, confirm it is healthy:

```bash
rpeek doctor
```

A healthy setup returns `"ok": true`. If `rpeek` is missing or `doctor`
fails, install it from <https://github.com/bbuchsbaum/rpeek> and ensure R and
the R package `jsonlite` are available.

## Core workflow

Always prefer the highest-level command that answers the question, then drill
in only if needed.

1. **Orient on a package**

   ```bash
   rpeek map <package>
   ```

   One-shot orientation pass. Covers metadata, exported symbols, and common
   topics. Use this first when starting work on an unfamiliar package.

2. **Find a symbol when you only know a fragment**

   ```bash
   rpeek search <package> <query>          # within one package
   rpeek search-all <query>                # across all installed packages
   rpeek search --kind object --limit 10 <package> <query>
   ```

3. **Resolve and summarize a known symbol**

   ```bash
   rpeek resolve <name>                    # likely objects and topics
   rpeek summary <package> <name>          # one compact payload
   ```

4. **Drill into a single object**

   ```bash
   rpeek sig <package> <name>              # function signature
   rpeek source <package> <name>           # best-effort source
   rpeek doc <package> <name>              # installed help
   rpeek methods <package> <name>          # S3/S4 methods
   ```

5. **Look at functions in bulk**

   ```bash
   rpeek sigs <package>                    # exported function signatures
   rpeek sigs --all-objects <package>      # include non-exported namespace
   rpeek exports <package>                 # exported symbols only
   ```

6. **Cross-package questions**

   ```bash
   rpeek methods-across <generic> --package <pkg> --package <pkg>
   rpeek bridge <pkgA> <pkgB>              # dependency / overlap summary
   rpeek xref <package> <symbol>           # symbol references
   rpeek used-by <package> <symbol>        # callers
   ```

7. **Vignettes**

   ```bash
   rpeek vignettes <package>
   rpeek vignette <package> <name>
   rpeek search-vignettes <package> <query>
   ```

8. **Last-resort file search**

   ```bash
   rpeek grep <package> <query>            # search installed package files
   ```

## Speed tips

- For repeated queries on the same package, pre-index it once:

  ```bash
  rpeek index package <package>
  rpeek index show <package>
  rpeek index search <package> <query>
  ```

- To keep payloads small, add `--max-bytes <N>` and `--no-examples`:

  ```bash
  rpeek --max-bytes 4000 --no-examples doc stats lm
  ```

- To run several requests in one process, pipe NDJSON to `rpeek batch`:

  ```bash
  printf '%s\n' \
    '{"action":"summary","package":"stats","name":"lm"}' \
    '{"action":"source","package":"stats","name":"lm"}' \
    | rpeek batch
  ```

- To force calls to share the same warm daemon, set `RPEEK_SOCKET`:

  ```bash
  export RPEEK_SOCKET=/tmp/rpeek-session.sock
  ```

## Output and error model

Output is JSON by default. Parse fields directly rather than scraping prose.

Successful response shape:

```json
{ "schema_version": 1, "command": "summary", "ok": true, "payload": { "...": "..." } }
```

Errors are structured and often include `suggestions` and a `hint` pointing at
the next command to try. Treat a non-zero exit code as a real failure: `2`
means a request-level error (use the suggestions); `1` means a client or
runtime failure (re-check `rpeek doctor`).

## Saving local workflow knowledge

When you discover a non-obvious workflow that mixes a few packages, store it
so future sessions can find it:

```bash
rpeek snippet add --title "<short title>" \
  --package <pkg> --package <pkg> --tag workflow \
  --body "<one or two sentences>"
rpeek snippet search "<query>"
rpeek snippet list --package <pkg>
```

## Anti-patterns

- Do not start `Rscript` to ask "what does function X look like" when
  `rpeek sig`, `rpeek summary`, or `rpeek doc` will answer in milliseconds.
- Do not `grep` the installed library tree manually before trying
  `rpeek search`, `rpeek search-all`, or `rpeek grep`.
- Do not pass `--no-daemon` for normal use - it disables the warm cache that
  makes follow-up calls fast.
