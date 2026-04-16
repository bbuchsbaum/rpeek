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

all_help_topics <- function(package) {
  alias_map <- package_alias_map(package)
  sort(unique(c(names(alias_map), unname(alias_map))))
}

canonical_help_topics <- function(package) {
  alias_map <- package_alias_map(package)
  topics <- sort(unique(unname(alias_map)))
  if (!length(topics)) {
    return(all_help_topics(package))
  }
  topics
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
  split_field <- function(field) {
    value <- unname(desc[[field]] %||% "")
    if (!nzchar(value)) character() else strsplit(value, "\\s*,\\s*")[[1]]
  }
  list(
    package = package,
    version = unname(desc[["Version"]] %||% NA_character_),
    libpath = dirname(find.package(package)),
    install_path = find.package(package),
    title = unname(desc[["Title"]] %||% NA_character_),
    description = unname(desc[["Description"]] %||% NA_character_),
    license = unname(desc[["License"]] %||% NA_character_),
    authors = unname(desc[["Author"]] %||% NA_character_),
    maintainer = unname(desc[["Maintainer"]] %||% NA_character_),
    encoding = unname(desc[["Encoding"]] %||% NA_character_),
    needs_compilation = unname(desc[["NeedsCompilation"]] %||% NA_character_),
    repository = unname(desc[["Repository"]] %||% NA_character_),
    url = split_field("URL"),
    depends = split_field("Depends"),
    imports = split_field("Imports"),
    suggests = split_field("Suggests"),
    linking_to = split_field("LinkingTo"),
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
  srcref_range <- tryCatch({
    if (is.null(srcref)) {
      NULL
    } else {
      list(
        start_line = as.integer(srcref[[1]]),
        start_column = as.integer(srcref[[5]]),
        end_line = as.integer(srcref[[3]]),
        end_column = as.integer(srcref[[6]])
      )
    }
  }, error = function(...) NULL)
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
    srcref_path = src_path,
    srcref_range = srcref_range
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

parse_arguments_text <- function(text) {
  if (is.null(text) || !nzchar(text)) {
    return(list())
  }
  lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
  starts <- grep("^[[:space:]]{0,3}[[:alnum:]_.]+([,[:space:]][[:alnum:]_.]+)*:", lines)
  if (!length(starts)) {
    return(list())
  }

  lapply(seq_along(starts), function(i) {
    start <- starts[[i]]
    end <- if (i == length(starts)) length(lines) else starts[[i + 1]] - 1
    block <- lines[start:end]
    first <- sub("^\\s+", "", block[[1]])
    name <- sub(":.*$", "", first)
    first_desc <- sub("^[^:]+:\\s*", "", first)
    rest <- if (length(block) > 1) block[-1] else character()
    description <- paste(trim_blank_edges(c(first_desc, rest)), collapse = "\n")
    list(
      name = name,
      description = description
    )
  })
}

source_from_srcref <- function(path, range) {
  if (is.null(range) || is.na(path) || !file.exists(path)) {
    return(NULL)
  }
  lines <- readLines(path, warn = FALSE)
  start_line <- max(1L, as.integer(range$start_line))
  end_line <- min(length(lines), as.integer(range$end_line))
  if (is.na(start_line) || is.na(end_line) || start_line > end_line) {
    return(NULL)
  }
  list(
    origin = normalizePath(path, winslash = "/", mustWork = FALSE),
    range = list(
      start_line = start_line,
      end_line = end_line,
      start_column = range$start_column,
      end_column = range$end_column
    ),
    text = paste(lines[start_line:end_line], collapse = "\n")
  )
}

best_effort_source <- function(package, name) {
  info <- lookup_object(package, name)
  obj <- info$object

  if (is.function(obj) || is.language(obj) || is.expression(obj)) {
    kind <- "deparsed"
    origin <- "installed_object"
    range <- NULL
    text <- paste(deparse(obj), collapse = "\n")
    srcref_source <- source_from_srcref(info$srcref_path, info$srcref_range)
    if (!is.null(srcref_source)) {
      text <- srcref_source$text
      kind <- "raw_file"
      origin <- srcref_source$origin
      range <- srcref_source$range
    }
    return(list(
      package = package,
      name = name,
      kind = kind,
      origin = origin,
      range = range,
      language = "R",
      text = text
    ))
  }

  list(
    package = package,
    name = name,
    kind = "unavailable",
    origin = "installed_object",
    range = NULL,
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
  arguments_text <- plain_sections[["Arguments"]] %||% section_text("\\arguments")

  list(
    package = package,
    topic = topic,
    aliases = aliases,
    title = section_text("\\title"),
    description = plain_sections[["Description"]] %||% section_text("\\description"),
    usage = plain_sections[["Usage"]] %||% section_text("\\usage"),
    arguments = arguments_text,
    arguments_detail = parse_arguments_text(arguments_text),
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
  ns <- asNamespace(package)
  s3_methods <- tryCatch(
    sort(unique(utils::methods(name))),
    error = function(...) character()
  )
  package_s3_methods <- tryCatch({
    registered <- get(".__S3MethodsTable__.", envir = ns, inherits = FALSE)
    package_methods <- ls(registered, all.names = TRUE)
    package_methods[startsWith(package_methods, paste0(name, "."))]
  }, error = function(...) character())
  s3_methods <- sort(unique(c(as.character(s3_methods), package_s3_methods)))
  s4_methods <- tryCatch({
    if (!methods::isGeneric(name, where = ns)) {
      character()
    } else {
      shown <- capture.output(show(methods::findMethods(name, where = ns)))
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

vignette_topic_from_file <- function(path) {
  sub("\\.[^.]+$", "", basename(path %||% ""))
}

vignette_index <- function(package) {
  pkg_path <- normalize_package(package)
  meta_path <- file.path(pkg_path, "Meta", "vignette.rds")
  if (!file.exists(meta_path)) {
    return(data.frame())
  }

  meta <- tryCatch(readRDS(meta_path), error = function(...) data.frame())
  if (!is.data.frame(meta) || !nrow(meta)) {
    return(data.frame())
  }

  if (!"File" %in% names(meta)) {
    meta$File <- rep(NA_character_, nrow(meta))
  }
  if (!"Title" %in% names(meta)) {
    meta$Title <- rep(NA_character_, nrow(meta))
  }
  if (!"R" %in% names(meta)) {
    meta$R <- rep(NA_character_, nrow(meta))
  }
  if (!"PDF" %in% names(meta)) {
    meta$PDF <- rep(NA_character_, nrow(meta))
  }

  topics <- vapply(meta$File, vignette_topic_from_file, character(1), USE.NAMES = FALSE)
  doc_dir <- file.path(pkg_path, "doc")
  file_paths <- file.path(doc_dir, meta$File)
  r_paths <- file.path(doc_dir, meta$R)
  pdf_paths <- file.path(doc_dir, meta$PDF)

  data.frame(
    Topic = topics,
    Title = as.character(meta$Title),
    File = as.character(meta$File),
    R = as.character(meta$R),
    PDF = as.character(meta$PDF),
    FilePath = ifelse(file.exists(file_paths), file_paths, NA_character_),
    RPath = ifelse(file.exists(r_paths), r_paths, NA_character_),
    PDFPath = ifelse(file.exists(pdf_paths), pdf_paths, NA_character_),
    stringsAsFactors = FALSE
  )
}

vignette_records <- function(package) {
  index <- vignette_index(package)
  if (!nrow(index)) {
    return(list())
  }

  lapply(seq_len(nrow(index)), function(i) {
    list(
      package = package,
      topic = index$Topic[[i]],
      title = index$Title[[i]],
      file = index$File[[i]],
      r = index$R[[i]],
      pdf = index$PDF[[i]],
      paths = list(
        source = index$FilePath[[i]],
        r = index$RPath[[i]],
        pdf = index$PDFPath[[i]]
      )
    )
  })
}

vignette_suggestions <- function(index, name) {
  candidates <- unique(c(index$Topic, index$Title, index$File))
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  candidate_suggestions(candidates, name)
}

resolve_vignette_row <- function(package, name) {
  index <- vignette_index(package)
  if (!nrow(index)) {
    rpkg_stop(
      "vignette_not_found",
      sprintf("package '%s' does not expose installed vignettes", package),
      hint = sprintf("Try `rpeek files %s` to inspect installed documentation files.", package)
    )
  }

  lowered <- tolower(name)
  topic_match <- which(tolower(index$Topic) == lowered)
  file_match <- which(tolower(index$File) == lowered)
  title_match <- which(tolower(index$Title) == lowered)
  base_match <- which(tolower(vapply(index$File, vignette_topic_from_file, character(1), USE.NAMES = FALSE)) == lowered)
  idx <- unique(c(topic_match, file_match, title_match, base_match))
  if (!length(idx)) {
    contains <- which(
      grepl(lowered, tolower(index$Topic), fixed = TRUE) |
        grepl(lowered, tolower(index$Title), fixed = TRUE) |
        grepl(lowered, tolower(index$File), fixed = TRUE)
    )
    if (length(contains) == 1) {
      idx <- contains
    }
  }

  if (!length(idx)) {
    rpkg_stop(
      "vignette_not_found",
      sprintf("vignette '%s' not found in package '%s'", name, package),
      suggestions = vignette_suggestions(index, name),
      hint = sprintf("Try `rpeek vignettes %s` or `rpeek search-vignettes %s %s`.", package, package, name)
    )
  }

  if (length(idx) > 1) {
    rpkg_stop(
      "vignette_ambiguous",
      sprintf("vignette '%s' matched multiple installed vignettes in package '%s'", name, package),
      suggestions = unique(index$Topic[idx]),
      hint = sprintf("Use one of the exact topics from `rpeek vignettes %s`.", package)
    )
  }

  index[idx[[1]], , drop = FALSE]
}

normalize_html_entities <- function(text) {
  text <- gsub("&nbsp;", " ", text, fixed = TRUE)
  text <- gsub("&lt;", "<", text, fixed = TRUE)
  text <- gsub("&gt;", ">", text, fixed = TRUE)
  text <- gsub("&amp;", "&", text, fixed = TRUE)
  text <- gsub("&quot;", "\"", text, fixed = TRUE)
  text
}

html_to_text <- function(text) {
  text <- gsub("(?is)<script[^>]*>.*?</script>", " ", text, perl = TRUE)
  text <- gsub("(?is)<style[^>]*>.*?</style>", " ", text, perl = TRUE)
  text <- gsub("(?i)<br\\s*/?>", "\n", text, perl = TRUE)
  text <- gsub("(?i)</(p|div|li|tr|h1|h2|h3|h4|h5|h6)>", "\n", text, perl = TRUE)
  text <- gsub("(?s)<[^>]+>", " ", text, perl = TRUE)
  text <- normalize_html_entities(text)
  lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines, which = "both")
  lines <- trim_blank_edges(lines[nzchar(lines)])
  paste(lines, collapse = "\n")
}

read_vignette_text_file <- function(path) {
  ext <- tolower(tools::file_ext(path))
  lines <- tryCatch(readLines(path, warn = FALSE, encoding = "UTF-8"), error = function(...) NULL)
  if (is.null(lines)) {
    return(NULL)
  }
  raw_text <- paste(lines, collapse = "\n")
  text <- if (ext %in% c("html", "htm")) html_to_text(raw_text) else raw_text
  list(
    path = normalizePath(path, winslash = "/", mustWork = FALSE),
    kind = ext %||% "text",
    text = text,
    lines = strsplit(text, "\n", fixed = TRUE)[[1]]
  )
}

best_vignette_text <- function(row) {
  candidates <- unique(na.omit(c(row$FilePath[[1]], row$RPath[[1]])))
  preferred <- candidates[order(match(tolower(tools::file_ext(candidates)), c("rnw", "rmd", "qmd", "md", "r", "html", "htm", "txt"), nomatch = 999L))]
  preferred <- preferred[!duplicated(preferred)]

  for (path in preferred) {
    text <- read_vignette_text_file(path)
    if (!is.null(text) && nzchar(text$text)) {
      return(text)
    }
  }

  NULL
}

list_vignettes <- function(package) {
  list(
    package = package,
    vignettes = vignette_records(package),
    counts = list(vignettes = nrow(vignette_index(package)))
  )
}

read_vignette <- function(package, name) {
  row <- resolve_vignette_row(package, name)
  text_source <- best_vignette_text(row)

  list(
    package = package,
    topic = row$Topic[[1]],
    title = row$Title[[1]],
    file = row$File[[1]],
    r = row$R[[1]],
    pdf = row$PDF[[1]],
    paths = list(
      source = row$FilePath[[1]],
      r = row$RPath[[1]],
      pdf = row$PDFPath[[1]]
    ),
    text = text_source$text %||% NULL,
    text_path = text_source$path %||% NULL,
    text_kind = text_source$kind %||% NULL
  )
}

search_vignette_text <- function(package, query, limit = 25) {
  index <- vignette_index(package)
  limit <- suppressWarnings(as.integer(limit))
  if (is.na(limit) || limit < 1) {
    limit <- 25
  }
  limit <- min(limit, 200)

  matches <- list()
  scanned_files <- 0L

  if (!nrow(index)) {
    return(list(
      package = package,
      query = query,
      limit = limit,
      counts = list(vignettes = 0, scanned_files = 0),
      matches = matches,
      truncated = FALSE
    ))
  }

  for (i in seq_len(nrow(index))) {
    topic <- index$Topic[[i]]
    title <- index$Title[[i]]
    file <- index$File[[i]]
    metadata_fields <- c(topic, title, file)
    if (any(grepl(query, metadata_fields, fixed = TRUE, ignore.case = TRUE), na.rm = TRUE)) {
      matches[[length(matches) + 1L]] <- list(
        topic = topic,
        title = title,
        matched_in = "metadata",
        path = index$FilePath[[i]] %||% index$RPath[[i]],
        line = NULL,
        text = NULL
      )
      if (length(matches) >= limit) {
        return(list(
          package = package,
          query = query,
          limit = limit,
          counts = list(vignettes = nrow(index), scanned_files = scanned_files),
          matches = matches,
          truncated = TRUE
        ))
      }
    }

    text_source <- best_vignette_text(index[i, , drop = FALSE])
    if (is.null(text_source)) {
      next
    }
    scanned_files <- scanned_files + 1L
    line_hits <- grep(query, text_source$lines, fixed = TRUE, ignore.case = TRUE)
    if (!length(line_hits)) {
      next
    }
    for (line_no in line_hits) {
      matches[[length(matches) + 1L]] <- list(
        topic = topic,
        title = title,
        matched_in = "content",
        path = text_source$path,
        line = as.integer(line_no),
        text = text_source$lines[[line_no]]
      )
      if (length(matches) >= limit) {
        return(list(
          package = package,
          query = query,
          limit = limit,
          counts = list(vignettes = nrow(index), scanned_files = scanned_files),
          matches = matches,
          truncated = TRUE
        ))
      }
    }
  }

  list(
    package = package,
    query = query,
    limit = limit,
    counts = list(vignettes = nrow(index), scanned_files = scanned_files),
    matches = matches,
    truncated = FALSE
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

package_signatures <- function(package, all_objects = FALSE) {
  normalize_package(package)
  ns <- asNamespace(package)
  object_names <- list_objects(package, exports_only = !isTRUE(all_objects))
  exports <- getNamespaceExports(package)
  signatures <- list()

  for (name in object_names) {
    obj <- tryCatch(get(name, envir = ns, inherits = FALSE), error = function(...) NULL)
    if (!is.function(obj)) {
      next
    }
    signatures[[length(signatures) + 1L]] <- list(
      package = package,
      name = name,
      exported = name %in% exports,
      type = typeof(obj),
      class = as.character(class(obj)),
      mode = mode(obj),
      signature = format_signature(obj)
    )
  }

  list(
    package = package,
    all_objects = isTRUE(all_objects),
    counts = list(
      scanned = length(object_names),
      signatures = length(signatures)
    ),
    signatures = signatures
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
  topics <- all_help_topics(package)

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

enrich_resolve_match <- function(record) {
  package <- record[["package"]]
  if (is.null(package)) {
    return(record)
  }
  label <- record[["name"]] %||% record[["topic"]]
  if (is.null(label)) {
    return(record)
  }
  record$has_doc <- !is.null(tryCatch(help_topic_summary(package, label), error = function(...) NULL))
  record$has_source <- if (identical(record[["kind"]], "object")) {
    !identical(tryCatch(best_effort_source(package, label)$kind, error = function(...) "unavailable"), "unavailable")
  } else {
    FALSE
  }
  record
}

resolve_query <- function(query, package = NULL, kind = "all", limit = 10) {
  kind <- match.arg(kind, c("all", "object", "topic"))
  limit <- suppressWarnings(as.integer(limit))
  if (is.na(limit) || limit < 1) {
    limit <- 10
  }
  limit <- min(limit, 50)

  base <- if (!is.null(package) && nzchar(package)) {
    result <- search_package(package, query, kind = kind, limit = limit)
    result$matches <- lapply(result$matches, function(record) {
      record$package <- package
      record
    })
    result$scope <- "package"
    result
  } else {
    search_all_packages(query, kind = kind, limit = limit)
  }

  matches <- base$matches
  if (length(matches)) {
    matches <- lapply(matches, enrich_resolve_match)
  }

  list(
    query = query,
    package = package,
    kind = kind,
    limit = limit,
    scope = base$scope %||% "installed_packages",
    candidates = matches,
    counts = base$counts
  )
}

path_matches_glob <- function(path, glob) {
  if (is.null(glob) || !nzchar(glob)) {
    return(TRUE)
  }
  grepl(utils::glob2rx(glob), path)
}

grep_package_files <- function(package, query, glob = NULL, limit = 25) {
  pkg_path <- normalize_package(package)
  limit <- suppressWarnings(as.integer(limit))
  if (is.na(limit) || limit < 1) {
    limit <- 25
  }
  limit <- min(limit, 200)

  files <- list.files(pkg_path, recursive = TRUE, all.files = TRUE, no.. = TRUE)
  files <- files[path_matches_glob(files, glob)]
  matches <- list()
  scanned <- 0L

  for (rel in files) {
    full <- file.path(pkg_path, rel)
    if (!file.info(full)$isdir && file.info(full)$size <= 1024 * 1024) {
      text <- tryCatch(readLines(full, warn = FALSE, encoding = "UTF-8"), error = function(...) NULL)
      if (is.null(text)) {
        next
      }
      scanned <- scanned + 1L
      idx <- grep(query, text, fixed = TRUE, ignore.case = TRUE)
      if (length(idx)) {
        for (line_no in idx) {
          matches[[length(matches) + 1L]] <- list(
            path = rel,
            line = as.integer(line_no),
            text = text[[line_no]]
          )
          if (length(matches) >= limit) {
            return(list(
              package = package,
              install_path = pkg_path,
              query = query,
              glob = glob,
              scanned_files = scanned,
              matches = matches,
              truncated = TRUE
            ))
          }
        }
      }
    }
  }

  list(
    package = package,
    install_path = pkg_path,
    query = query,
    glob = glob,
    scanned_files = scanned,
    matches = matches,
    truncated = FALSE
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
      range = source$range,
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
  glob <- req[["glob"]]

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
    "resolve" = resolve_query(query, package = package, kind = kind, limit = limit),
    "summary" = summary_for_object(package, name),
    "sig" = object_info(package, name),
    "sigs" = package_signatures(package, all_objects = req[["all_objects"]] %||% FALSE),
    "source" = best_effort_source(package, name),
    "doc" = extract_help_topic(package, topic %||% name),
    "topics" = list(package = package, topics = canonical_help_topics(package)),
    "methods" = list_methods(package, name),
    "vignettes" = list_vignettes(package),
    "vignette" = read_vignette(package, name),
    "search_vignettes" = search_vignette_text(package, query, limit = limit),
    "files" = list_files(package),
    "grep" = grep_package_files(package, query, glob = glob, limit = limit),
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
