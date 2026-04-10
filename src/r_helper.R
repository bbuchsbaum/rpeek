options(
  warn = 1,
  keep.source = TRUE,
  keep.source.pkgs = TRUE
)

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop(
    "rpeek requires the 'jsonlite' package for helper protocol support. Install it with install.packages('jsonlite').",
    call. = FALSE
  )
}

rpeek_state <- new.env(parent = emptyenv())
rpeek_state$search_index <- new.env(parent = emptyenv())

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

decode_request <- function(line) {
  line <- sub("^\\s+", "", line)
  line <- sub("\\s+$", "", line)
  if (!nzchar(line)) {
    stop("empty request")
  }

  req <- jsonlite::fromJSON(line, simplifyVector = FALSE)
  if (!is.list(req)) {
    stop("request must decode to an object")
  }

  req
}

to_json <- function(x) {
  jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    POSIXt = "ISO8601",
    digits = NA
  )
}

normalize_package <- function(package) {
  tryCatch({
    suppressPackageStartupMessages(loadNamespace(package))
    find.package(package)
  }, error = function(err) {
    rpkg_stop(
      "package_not_found",
      sprintf("package '%s' is not installed", package),
      hint = "Check installed packages or library paths."
    )
  })
}

rpkg_stop <- function(code, message, suggestions = NULL, hint = NULL) {
  err <- simpleError(message)
  class(err) <- c("rpkg_error", class(err))
  attr(err, "rpkg_code") <- code
  attr(err, "rpkg_suggestions") <- suggestions
  attr(err, "rpkg_hint") <- hint
  stop(err)
}

package_alias_map <- function(package) {
  pkg_path <- normalize_package(package)
  alias_path <- file.path(pkg_path, "help", "aliases.rds")
  if (!file.exists(alias_path)) {
    return(setNames(character(), character()))
  }
  readRDS(alias_path)
}

all_object_names <- function(package) {
  sort(ls(asNamespace(package), all.names = TRUE))
}

installed_package_info <- function() {
  info <- utils::installed.packages()[, c("Package", "LibPath"), drop = FALSE]
  info <- info[order(info[, "Package"]), , drop = FALSE]
  rownames(info) <- NULL
  info
}

read_package_alias_map <- function(install_path) {
  alias_path <- file.path(install_path, "help", "aliases.rds")
  if (!file.exists(alias_path)) {
    return(setNames(character(), character()))
  }
  tryCatch(readRDS(alias_path), error = function(...) setNames(character(), character()))
}

read_namespace_exports <- function(install_path) {
  nsinfo_path <- file.path(install_path, "Meta", "nsInfo.rds")
  if (!file.exists(nsinfo_path)) {
    return(character())
  }

  nsinfo <- tryCatch(readRDS(nsinfo_path), error = function(...) NULL)
  exports <- nsinfo[["exports"]]
  if (is.null(exports)) {
    return(character())
  }

  sort(unique(as.character(exports)))
}

package_search_index <- function(package, install_path) {
  key <- normalizePath(install_path, winslash = "/", mustWork = FALSE)
  if (exists(key, envir = rpeek_state$search_index, inherits = FALSE)) {
    return(get(key, envir = rpeek_state$search_index, inherits = FALSE))
  }

  alias_map <- read_package_alias_map(install_path)
  index <- list(
    package = package,
    install_path = install_path,
    exports = read_namespace_exports(install_path),
    topics = sort(unique(c(names(alias_map), unname(alias_map))))
  )
  assign(key, index, envir = rpeek_state$search_index)
  index
}

rank_candidates <- function(candidates, query) {
  candidates <- unique(candidates[nzchar(candidates)])
  if (!length(candidates)) {
    return(character())
  }

  lowered <- tolower(candidates)
  query_lower <- tolower(query)
  exact <- lowered == query_lower
  prefix <- startsWith(lowered, query_lower)
  contains <- grepl(query_lower, lowered, fixed = TRUE)
  distances <- as.integer(utils::adist(query_lower, lowered, partial = TRUE))
  candidates[order(!exact, !prefix, !contains, distances, nchar(candidates), candidates)]
}

candidate_suggestions <- function(candidates, query, limit = 5) {
  head(rank_candidates(candidates, query), limit)
}

object_suggestions <- function(package, name) {
  candidate_suggestions(all_object_names(package), name)
}

topic_suggestions <- function(package, topic) {
  alias_map <- package_alias_map(package)
  candidates <- unique(c(names(alias_map), unname(alias_map)))
  candidate_suggestions(candidates, topic)
}

package_description <- function(package) {
  desc <- packageDescription(package)
  exports <- tryCatch(
    getNamespaceExports(package),
    error = function(...) character()
  )
  list(
    package = package,
    version = unname(desc[["Version"]] %||% NA_character_),
    libpath = dirname(find.package(package)),
    install_path = find.package(package),
    title = unname(desc[["Title"]] %||% NA_character_),
    repository = unname(desc[["Repository"]] %||% NA_character_),
    url = strsplit(unname(desc[["URL"]] %||% ""), "\\s*,\\s*")[[1]],
    depends = strsplit(unname(desc[["Depends"]] %||% ""), "\\s*,\\s*")[[1]],
    imports = strsplit(unname(desc[["Imports"]] %||% ""), "\\s*,\\s*")[[1]],
    suggests = strsplit(unname(desc[["Suggests"]] %||% ""), "\\s*,\\s*")[[1]],
    exports = sort(exports)
  )
}

lookup_object <- function(package, name) {
  ns <- asNamespace(package)
  exists_in_ns <- exists(name, envir = ns, inherits = FALSE)
  if (!exists_in_ns) {
    suggestions <- object_suggestions(package, name)
    rpkg_stop(
      "object_not_found",
      sprintf("object '%s' not found in package '%s'", name, package),
      suggestions = suggestions,
      hint = sprintf("Try `rpeek search %s %s`.", package, name)
    )
  }
  obj <- get(name, envir = ns, inherits = FALSE)
  srcref <- attr(obj, "srcref", exact = TRUE)
  src_path <- tryCatch({
    path <- utils::getSrcref(srcref)
    if (length(path) == 0) NA_character_ else as.character(path[[1]])
  }, error = function(...) NA_character_)

  list(
    object = obj,
    exported = name %in% getNamespaceExports(package),
    type = typeof(obj),
    class = class(obj),
    mode = mode(obj),
    srcref_path = src_path
  )
}

format_signature <- function(obj) {
  if (!is.function(obj)) {
    return(NULL)
  }
  paste(capture.output(args(obj)), collapse = "\n")
}

strip_rd_overstrike <- function(text) {
  gsub(".\\x08", "", text, perl = TRUE)
}

trim_blank_edges <- function(lines) {
  while (length(lines) && !nzchar(trimws(lines[[1]]))) {
    lines <- lines[-1]
  }
  while (length(lines) && !nzchar(trimws(lines[[length(lines)]]))) {
    lines <- lines[-length(lines)]
  }
  lines
}

plain_rd_sections <- function(text) {
  lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
  header_idx <- grep("^[[:alpha:]][[:alnum:][:space:]/()_-]*:$", lines)
  if (!length(header_idx)) {
    return(list())
  }

  sections <- list()
  for (i in seq_along(header_idx)) {
    start <- header_idx[[i]]
    end <- if (i == length(header_idx)) length(lines) else header_idx[[i + 1]] - 1
    title <- sub(":$", "", lines[[start]])
    body <- if (start < end) trim_blank_edges(lines[(start + 1):end]) else character()
    sections[[title]] <- paste(body, collapse = "\n")
  }
  sections
}

best_effort_source <- function(package, name) {
  info <- lookup_object(package, name)
  obj <- info$object

  if (is.function(obj) || is.language(obj) || is.expression(obj)) {
    kind <- "deparsed"
    origin <- "installed_object"
    text <- paste(deparse(obj), collapse = "\n")
    if (!is.na(info$srcref_path) && file.exists(info$srcref_path)) {
      text <- paste(readLines(info$srcref_path, warn = FALSE), collapse = "\n")
      kind <- "raw_file"
      origin <- normalizePath(info$srcref_path, winslash = "/", mustWork = FALSE)
    }
    return(list(
      package = package,
      name = name,
      kind = kind,
      origin = origin,
      language = "R",
      text = text
    ))
  }

  list(
    package = package,
    name = name,
    kind = "unavailable",
    origin = "installed_object",
    language = NULL,
    text = NULL
  )
}

extract_help_topic <- function(package, topic) {
  path <- suppressWarnings(do.call(utils::help, list(topic = topic, package = package)))
  if (length(path) == 0) {
    suggestions <- topic_suggestions(package, topic)
    rpkg_stop(
      "topic_not_found",
      sprintf("help topic '%s' not found in package '%s'", topic, package),
      suggestions = suggestions,
      hint = sprintf("Try `rpeek search %s %s`.", package, topic)
    )
  }

  rd <- utils:::.getHelpFile(path)
  plain_text <- strip_rd_overstrike(
    paste(capture.output(tools::Rd2txt(rd, options = list(width = 80))), collapse = "\n")
  )
  plain_sections <- plain_rd_sections(plain_text)
  tags <- vapply(rd, attr, character(1), "Rd_tag")
  flatten_rd <- function(x) {
    if (is.character(x)) {
      return(paste(x, collapse = ""))
    }
    if (is.list(x)) {
      parts <- vapply(x, flatten_rd, character(1), USE.NAMES = FALSE)
      return(paste(parts, collapse = ""))
    }
    ""
  }
  section_text <- function(tag) {
    idx <- which(tags == tag)
    if (!length(idx)) {
      return(NULL)
    }
    text <- vapply(rd[idx], flatten_rd, character(1), USE.NAMES = FALSE)
    paste(trimws(text), collapse = "\n")
  }

  aliases <- unlist(strsplit(section_text("\\alias") %||% "", "\n", fixed = TRUE))
  aliases <- trimws(aliases)
  aliases <- aliases[nzchar(aliases)]

  list(
    package = package,
    topic = topic,
    aliases = aliases,
    title = section_text("\\title"),
    description = plain_sections[["Description"]] %||% section_text("\\description"),
    usage = plain_sections[["Usage"]] %||% section_text("\\usage"),
    arguments = plain_sections[["Arguments"]] %||% section_text("\\arguments"),
    value = plain_sections[["Value"]] %||% section_text("\\value"),
    examples = plain_sections[["Examples"]] %||% section_text("\\examples"),
    text = plain_text
  )
}

list_objects <- function(package, exports_only = FALSE) {
  normalize_package(package)
  if (exports_only) {
    sort(getNamespaceExports(package))
  } else {
    sort(ls(asNamespace(package), all.names = TRUE))
  }
}

list_methods <- function(package, name) {
  normalize_package(package)
  s3_methods <- tryCatch(
    sort(unique(utils::methods(name))),
    error = function(...) character()
  )
  s4_methods <- tryCatch({
    if (!methods::isGeneric(name)) {
      character()
    } else {
      shown <- capture.output(show(methods::findMethods(name)))
      shown[nzchar(trimws(shown))]
    }
  }, error = function(...) character())

  list(
    package = package,
    generic = name,
    s3_methods = s3_methods,
    s4_methods = s4_methods
  )
}

list_files <- function(package) {
  pkg_path <- normalize_package(package)
  files <- list.files(pkg_path, recursive = TRUE, all.files = TRUE, no.. = TRUE)
  list(
    package = package,
    install_path = pkg_path,
    files = files
  )
}

object_info <- function(package, name) {
  info <- lookup_object(package, name)
  list(
    package = package,
    name = name,
    exported = isTRUE(info$exported),
    type = info$type,
    class = as.character(info$class),
    mode = info$mode,
    signature = format_signature(info$object)
  )
}

help_topic_summary <- function(package, topic) {
  doc <- tryCatch(
    extract_help_topic(package, topic),
    error = function(...) NULL
  )
  if (is.null(doc)) {
    return(NULL)
  }

  list(
    topic = doc$topic,
    aliases = doc$aliases,
    title = doc$title,
    description = doc$description
  )
}

search_matches <- function(candidates, query, limit, builder) {
  candidates <- unique(candidates[nzchar(candidates)])
  if (!length(candidates)) {
    return(list(matches = list(), total = 0, matched_by = "none"))
  }

  lowered <- tolower(candidates)
  query_lower <- tolower(query)
  substring_matches <- candidates[grepl(query_lower, lowered, fixed = TRUE)]
  matched_by <- "substring"
  pool <- substring_matches

  if (!length(pool)) {
    matched_by <- "fuzzy"
    pool <- head(rank_candidates(candidates, query), max(limit * 3, limit))
  }

  ranked <- head(rank_candidates(pool, query), limit)
  list(
    matches = lapply(ranked, function(item) builder(item, matched_by)),
    total = length(pool),
    matched_by = matched_by
  )
}

record_label <- function(record) {
  record[["name"]] %||% record[["topic"]] %||% ""
}

rank_record_indices <- function(records, query) {
  if (!length(records)) {
    return(integer())
  }

  labels <- vapply(records, record_label, character(1), USE.NAMES = FALSE)
  lowered <- tolower(labels)
  query_lower <- tolower(query)
  exact <- lowered == query_lower
  prefix <- startsWith(lowered, query_lower)
  contains <- grepl(query_lower, lowered, fixed = TRUE)
  distances <- as.integer(utils::adist(query_lower, lowered, partial = TRUE))
  packages <- vapply(records, function(record) record[["package"]] %||% "", character(1), USE.NAMES = FALSE)

  order(!exact, !prefix, !contains, distances, nchar(labels), packages, labels)
}

search_record_matches <- function(records, query, limit) {
  if (!length(records)) {
    return(list(matches = list(), total = 0, matched_by = "none"))
  }

  labels <- vapply(records, record_label, character(1), USE.NAMES = FALSE)
  lowered <- tolower(labels)
  query_lower <- tolower(query)
  substring_idx <- which(grepl(query_lower, lowered, fixed = TRUE))
  matched_by <- "substring"
  pool <- substring_idx

  if (!length(pool)) {
    matched_by <- "fuzzy"
    ordered <- rank_record_indices(records, query)
    pool <- head(ordered, max(limit * 3, limit))
  }

  if (!length(pool)) {
    return(list(matches = list(), total = 0, matched_by = matched_by))
  }

  pool_records <- records[pool]
  ranked <- pool_records[rank_record_indices(pool_records, query)]
  list(
    matches = unname(head(ranked, limit)),
    total = length(pool_records),
    matched_by = matched_by
  )
}

search_all_packages <- function(query, kind = "all", limit = 25) {
  kind <- match.arg(kind, c("all", "object", "topic"))
  limit <- suppressWarnings(as.integer(limit))
  if (is.na(limit) || limit < 1) {
    limit <- 25
  }
  limit <- min(limit, 100)

  package_info <- installed_package_info()
  object_records <- list()
  topic_records <- list()

  for (i in seq_len(nrow(package_info))) {
    package <- package_info[i, "Package"]
    install_path <- file.path(package_info[i, "LibPath"], package)
    index <- tryCatch(package_search_index(package, install_path), error = function(...) NULL)
    if (is.null(index)) {
      next
    }

    if (kind %in% c("all", "object") && length(index$exports)) {
      object_records <- c(object_records, lapply(index$exports, function(name) {
        list(
          kind = "object",
          package = package,
          name = name,
          exported = TRUE
        )
      }))
    }

    if (kind %in% c("all", "topic") && length(index$topics)) {
      topic_records <- c(topic_records, lapply(index$topics, function(topic) {
        list(
          kind = "topic",
          package = package,
          topic = topic
        )
      }))
    }
  }

  object_results <- search_record_matches(object_records, query, limit)
  topic_results <- search_record_matches(topic_records, query, limit)
  matches <- c(object_results$matches, topic_results$matches)
  if (length(matches)) {
    matches <- unname(matches[rank_record_indices(matches, query)])
    matches <- head(matches, limit)
  }

  matched_packages <- if (length(matches)) {
    length(unique(vapply(matches, function(record) record[["package"]], character(1), USE.NAMES = FALSE)))
  } else {
    0
  }

  list(
    query = query,
    kind = kind,
    limit = limit,
    scope = "installed_packages",
    matches = matches,
    counts = list(
      packages = nrow(package_info),
      objects = object_results$total,
      topics = topic_results$total,
      matched_packages = matched_packages
    )
  )
}

search_package <- function(package, query, kind = "all", limit = 25) {
  normalize_package(package)
  kind <- match.arg(kind, c("all", "object", "topic"))
  limit <- suppressWarnings(as.integer(limit))
  if (is.na(limit) || limit < 1) {
    limit <- 25
  }
  limit <- min(limit, 100)

  objects <- all_object_names(package)
  alias_map <- package_alias_map(package)
  topics <- unique(c(names(alias_map), unname(alias_map)))

  object_results <- list(matches = list(), total = 0, matched_by = "none")
  topic_results <- list(matches = list(), total = 0, matched_by = "none")

  if (kind %in% c("all", "object")) {
    object_results <- search_matches(objects, query, limit, function(name, matched_by) {
      list(
        kind = "object",
        name = name,
        exported = name %in% getNamespaceExports(package),
        matched_by = matched_by
      )
    })
  }

  if (kind %in% c("all", "topic")) {
    topic_results <- search_matches(topics, query, limit, function(topic, matched_by) {
      list(
        kind = "topic",
        topic = topic,
        matched_by = matched_by
      )
    })
  }

  list(
    package = package,
    query = query,
    kind = kind,
    limit = limit,
    matches = c(object_results$matches, topic_results$matches),
    counts = list(
      objects = object_results$total,
      topics = topic_results$total
    )
  )
}

summary_for_object <- function(package, name) {
  info <- object_info(package, name)
  source <- best_effort_source(package, name)
  doc <- help_topic_summary(package, name)
  methods <- tryCatch(
    list_methods(package, name),
    error = function(...) list(
      package = package,
      generic = name,
      s3_methods = character(),
      s4_methods = character()
    )
  )

  list(
    package = package,
    name = name,
    object = info,
    source = list(
      kind = source$kind,
      origin = source$origin,
      language = source$language
    ),
    doc = doc,
    methods = methods
  )
}

dispatch <- function(req) {
  package <- req[["package"]]
  name <- req[["name"]]
  query <- req[["query"]]
  kind <- req[["kind"]] %||% "all"
  limit <- req[["limit"]] %||% "25"
  topic <- req[["topic"]]

  payload <- switch(
    req[["action"]],
    "ping" = list(status = "ok"),
    "fingerprint" = list(
      package = package,
      install_path = normalize_package(package),
      version = as.character(utils::packageVersion(package))
    ),
    "pkg" = package_description(package),
    "exports" = list(package = package, exports = list_objects(package, TRUE)),
    "objects" = list(package = package, objects = list_objects(package, FALSE)),
    "search" = search_package(package, query, kind = kind, limit = limit),
    "search_all" = search_all_packages(query, kind = kind, limit = limit),
    "summary" = summary_for_object(package, name),
    "sig" = object_info(package, name),
    "source" = best_effort_source(package, name),
    "doc" = extract_help_topic(package, topic %||% name),
    "methods" = list_methods(package, name),
    "files" = list_files(package),
    stop(sprintf("unknown action '%s'", req[["action"]]))
  )

  list(
    schema_version = 1,
    ok = TRUE,
    payload = payload
  )
}

emit_error <- function(code, message) {
  list(
    schema_version = 1,
    ok = FALSE,
    error = list(code = code, message = message)
  )
}

stdin_conn <- file("stdin", open = "r")
on.exit(close(stdin_conn), add = TRUE)

repeat {
  line <- readLines(stdin_conn, n = 1, warn = FALSE)
  if (length(line) == 0) {
    break
  }

  response <- tryCatch(
    dispatch(decode_request(line[[1]])),
    error = function(err) {
      if (inherits(err, "rpkg_error")) {
        out <- emit_error(
          attr(err, "rpkg_code") %||% "r_error",
          conditionMessage(err)
        )
        out$error$suggestions <- attr(err, "rpkg_suggestions") %||% character()
        out$error$hint <- attr(err, "rpkg_hint")
        return(out)
      }

      emit_error("r_error", conditionMessage(err))
    }
  )

  cat(to_json(response), "\n", sep = "")
  flush.console()
}
