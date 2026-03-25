options(
  warn = 1,
  keep.source = TRUE,
  keep.source.pkgs = TRUE
)

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

json_escape <- function(x) {
  x <- enc2utf8(as.character(x))
  x <- gsub("\\\\", "\\\\\\\\", x, perl = TRUE)
  x <- gsub("\"", "\\\\\"", x, perl = TRUE)
  x <- gsub("\b", "\\\\b", x, perl = TRUE)
  x <- gsub("\f", "\\\\f", x, perl = TRUE)
  x <- gsub("\n", "\\\\n", x, perl = TRUE)
  x <- gsub("\r", "\\\\r", x, perl = TRUE)
  x <- gsub("\t", "\\\\t", x, perl = TRUE)
  x
}

to_json <- function(x) {
  if (is.null(x)) {
    return("null")
  }

  if (isTRUE(x)) {
    return("true")
  }

  if (identical(x, FALSE)) {
    return("false")
  }

  if (is.atomic(x) && length(x) == 1 && !is.character(x)) {
    if (is.na(x)) {
      return("null")
    }
    return(as.character(x))
  }

  if (is.character(x) && length(x) == 1) {
    if (is.na(x)) {
      return("null")
    }
    return(sprintf("\"%s\"", json_escape(x)))
  }

  if (is.atomic(x) && is.null(names(x))) {
    values <- vapply(
      as.list(x),
      to_json,
      character(1),
      USE.NAMES = FALSE
    )
    return(sprintf("[%s]", paste(values, collapse = ",")))
  }

  if (is.list(x) && is.null(names(x))) {
    values <- vapply(x, to_json, character(1), USE.NAMES = FALSE)
    return(sprintf("[%s]", paste(values, collapse = ",")))
  }

  if (!is.null(names(x))) {
    items <- Map(
      function(name, value) sprintf("\"%s\":%s", json_escape(name), to_json(value)),
      names(x),
      as.list(x)
    )
    return(sprintf("{%s}", paste(items, collapse = ",")))
  }

  sprintf("\"%s\"", json_escape(paste(capture.output(str(x)), collapse = "\n")))
}

decode_request <- function(line) {
  line <- sub("^\\s+", "", line)
  line <- sub("\\s+$", "", line)
  if (!nzchar(line)) {
    stop("empty request")
  }

  parse_string <- function(pos) {
    if (substr(line, pos, pos) != "\"") {
      stop("expected string")
    }
    pos <- pos + 1
    buf <- character()
    while (pos <= nchar(line)) {
      ch <- substr(line, pos, pos)
      if (ch == "\"") {
        return(list(value = paste(buf, collapse = ""), pos = pos + 1))
      }
      if (ch == "\\") {
        pos <- pos + 1
        esc <- substr(line, pos, pos)
        mapped <- switch(
          esc,
          "\"" = "\"",
          "\\" = "\\",
          "/" = "/",
          "b" = "\b",
          "f" = "\f",
          "n" = "\n",
          "r" = "\r",
          "t" = "\t",
          stop("unsupported escape")
        )
        buf <- c(buf, mapped)
        pos <- pos + 1
      } else {
        buf <- c(buf, ch)
        pos <- pos + 1
      }
    }
    stop("unterminated string")
  }

  pos <- 1
  expect <- function(token) {
    if (substr(line, pos, pos + nchar(token) - 1) != token) {
      stop(sprintf("expected '%s'", token))
    }
    pos <<- pos + nchar(token)
  }
  skip_ws <- function() {
    while (pos <= nchar(line) && grepl("\\s", substr(line, pos, pos))) {
      pos <<- pos + 1
    }
  }

  skip_ws()
  expect("{")
  skip_ws()
  out <- list()
  repeat {
    parsed_key <- parse_string(pos)
    key <- parsed_key$value
    pos <- parsed_key$pos
    skip_ws()
    expect(":")
    skip_ws()
    if (substr(line, pos, pos + 3) == "null") {
      value <- NULL
      pos <- pos + 4
    } else {
      parsed_val <- parse_string(pos)
      value <- parsed_val$value
      pos <- parsed_val$pos
    }
    out[[key]] <- value
    skip_ws()
    ch <- substr(line, pos, pos)
    if (ch == "}") {
      pos <- pos + 1
      break
    }
    expect(",")
    skip_ws()
  }
  out
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

candidate_suggestions <- function(candidates, query, limit = 5) {
  candidates <- unique(candidates[nzchar(candidates)])
  if (!length(candidates)) {
    return(character())
  }

  lowered <- tolower(candidates)
  query_lower <- tolower(query)
  contains <- candidates[grepl(query_lower, lowered, fixed = TRUE)]
  if (length(contains)) {
    return(head(contains, limit))
  }

  distances <- utils::adist(query_lower, lowered, partial = TRUE)
  ordered <- candidates[order(distances, nchar(candidates), candidates)]
  head(ordered, limit)
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
      hint = sprintf("Try `rpkg search %s %s`.", package, name)
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
      hint = sprintf("Try `rpkg search %s %s`.", package, topic)
    )
  }

  rd <- utils:::.getHelpFile(path)
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

  aliases <- trimws(section_text("\\alias"))
  aliases <- aliases[nzchar(aliases)]

  list(
    package = package,
    topic = topic,
    aliases = aliases,
    title = section_text("\\title"),
    description = section_text("\\description"),
    usage = section_text("\\usage"),
    arguments = section_text("\\arguments"),
    value = section_text("\\value"),
    examples = section_text("\\examples"),
    text = paste(capture.output(tools::Rd2txt(rd, options = list(width = 80))), collapse = "\n")
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

search_package <- function(package, query) {
  normalize_package(package)
  objects <- all_object_names(package)
  alias_map <- package_alias_map(package)
  query_lower <- tolower(query)

  object_matches <- objects[grepl(query_lower, tolower(objects), fixed = TRUE)]
  alias_name_matches <- names(alias_map)[grepl(query_lower, tolower(names(alias_map)), fixed = TRUE)]
  alias_topic_matches <- unname(alias_map)[grepl(query_lower, tolower(unname(alias_map)), fixed = TRUE)]
  topics <- unique(c(alias_name_matches, alias_topic_matches))

  object_results <- lapply(head(object_matches, 25), function(name) {
    list(
      kind = "object",
      name = name,
      exported = name %in% getNamespaceExports(package)
    )
  })
  topic_results <- lapply(head(topics, 25), function(topic) {
    list(
      kind = "topic",
      topic = topic
    )
  })

  list(
    package = package,
    query = query,
    matches = c(object_results, topic_results),
    counts = list(
      objects = length(object_matches),
      topics = length(topics)
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
    "search" = search_package(package, query),
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
