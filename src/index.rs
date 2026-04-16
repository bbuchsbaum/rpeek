//! Persistent package index storage and search primitives.
//!
//! [`IndexStore`] is the library entry point for the on-disk SQLite index used by
//! `rpeek index ...` commands. It stores package fingerprints, indexed topic and
//! vignette content, selected package files, and an FTS-backed document table for
//! package-scoped search.
//!
//! Typical library use is:
//!
//! 1. open the default index or a test-specific database
//! 2. inspect counts or package summaries
//! 3. query indexed content with [`IndexStore::search_package_documents`]
//!
//! ```no_run
//! use rpeek::IndexStore;
//!
//! # fn main() -> anyhow::Result<()> {
//! let store = IndexStore::open_default()?;
//! let stats = store.stats()?;
//!
//! if stats.indexed_packages > 0 {
//!     let matches = store.search_package_documents("stats", "reshape", 5)?;
//!     for entry in matches {
//!         println!("{}:{} {}", entry.kind, entry.key, entry.snippet);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const INDEX_SCHEMA_VERSION: i64 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageIndexState {
    pub package: String,
    pub install_path: PathBuf,
    pub helper_fingerprint: Option<String>,
    pub local_fingerprint: String,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexStats {
    pub path: PathBuf,
    pub schema_version: i64,
    pub package_count: usize,
    pub indexed_packages: usize,
    pub topic_count: usize,
    pub vignette_count: usize,
    pub file_count: usize,
    pub snippet_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexedTopic {
    pub topic: String,
    pub title: Option<String>,
    pub aliases: Vec<String>,
    pub description: Option<String>,
    pub usage: Option<String>,
    pub value: Option<String>,
    pub examples: Option<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexedVignette {
    pub topic: String,
    pub title: Option<String>,
    pub source_path: Option<String>,
    pub r_path: Option<String>,
    pub pdf_path: Option<String>,
    pub text_kind: Option<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexedFile {
    pub path: String,
    pub text_kind: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexedPackageRecord {
    pub package: String,
    pub version: Option<String>,
    pub title: Option<String>,
    pub install_path: PathBuf,
    pub package_json: Value,
    pub exports: Vec<String>,
    pub objects: Vec<String>,
    pub signatures_json: Value,
    pub topics: Vec<IndexedTopic>,
    pub vignettes: Vec<IndexedVignette>,
    pub files: Vec<IndexedFile>,
    pub indexed_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexedPackageData {
    pub package: String,
    pub version: Option<String>,
    pub title: Option<String>,
    pub install_path: PathBuf,
    pub package_json: Value,
    pub exports: Vec<String>,
    pub objects: Vec<String>,
    pub signatures_json: Value,
    pub indexed_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedPackageSummary {
    pub package: String,
    pub version: Option<String>,
    pub title: Option<String>,
    pub install_path: PathBuf,
    pub exports_count: usize,
    pub objects_count: usize,
    pub signatures_count: usize,
    pub topics_count: usize,
    pub vignettes_count: usize,
    pub files_count: usize,
    pub indexed_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexSearchMatch {
    pub kind: String,
    pub key: String,
    pub title: Option<String>,
    pub snippet: String,
    pub score: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedMethod {
    pub package: String,
    pub generic: String,
    pub method_name: String,
    pub class_name: Option<String>,
    pub system: String,
    pub source_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedPackageLink {
    pub package: String,
    pub related_package: String,
    pub relation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedCallRef {
    pub package: String,
    pub file_path: String,
    pub line_number: usize,
    pub caller_symbol: Option<String>,
    pub callee_package: Option<String>,
    pub callee_symbol: String,
    pub relation: String,
    pub snippet: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedSnippet {
    pub id: i64,
    pub title: String,
    pub body: String,
    pub packages: Vec<String>,
    pub objects: Vec<String>,
    pub tags: Vec<String>,
    pub verbs: Vec<String>,
    pub status: String,
    pub source: Option<String>,
    pub package_versions: BTreeMap<String, String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewSnippet {
    pub title: String,
    pub body: String,
    pub packages: Vec<String>,
    pub objects: Vec<String>,
    pub tags: Vec<String>,
    pub verbs: Vec<String>,
    pub status: String,
    pub source: Option<String>,
    pub package_versions: BTreeMap<String, String>,
}

pub struct IndexStore {
    conn: Connection,
    path: PathBuf,
}

impl IndexStore {
    pub fn open_default() -> Result<Self> {
        let path = default_index_path()?;
        Self::open(&path)
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create index directory {}", parent.display())
            })?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open index database {}", path.display()))?;
        let mut store = Self {
            conn,
            path: path.to_path_buf(),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn stats(&self) -> Result<IndexStats> {
        let schema_version = self.schema_version()?;
        let package_count = self.count("package_index_state")?;
        let indexed_packages = self.count("package_records")?;
        let topic_count = self.count("topics")?;
        let vignette_count = self.count("vignettes")?;
        let file_count = self.count("files")?;
        let snippet_count = self.count("snippets")?;
        Ok(IndexStats {
            path: self.path.clone(),
            schema_version,
            package_count,
            indexed_packages,
            topic_count,
            vignette_count,
            file_count,
            snippet_count,
        })
    }

    pub fn get_package_state(&self, package: &str) -> Result<Option<PackageIndexState>> {
        self.conn
            .query_row(
                "SELECT package, install_path, helper_fingerprint, local_fingerprint, updated_at
                 FROM package_index_state
                 WHERE package = ?1",
                [package],
                |row| {
                    Ok(PackageIndexState {
                        package: row.get(0)?,
                        install_path: PathBuf::from(row.get::<_, String>(1)?),
                        helper_fingerprint: row.get(2)?,
                        local_fingerprint: row.get(3)?,
                        updated_at: row.get(4)?,
                    })
                },
            )
            .optional()
            .context("failed to load indexed package state")
    }

    pub fn upsert_package_state(&self, state: &PackageIndexState) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO package_index_state
                   (package, install_path, helper_fingerprint, local_fingerprint, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(package) DO UPDATE SET
                   install_path = excluded.install_path,
                   helper_fingerprint = excluded.helper_fingerprint,
                   local_fingerprint = excluded.local_fingerprint,
                   updated_at = excluded.updated_at",
                params![
                    state.package,
                    state.install_path.display().to_string(),
                    state.helper_fingerprint,
                    state.local_fingerprint,
                    state.updated_at,
                ],
            )
            .context("failed to persist indexed package state")?;
        Ok(())
    }

    pub fn upsert_package_record(&mut self, record: &IndexedPackageRecord) -> Result<()> {
        let tx = self
            .conn
            .transaction()
            .context("failed to start index transaction")?;

        tx.execute(
            "DELETE FROM package_records WHERE package = ?1",
            [&record.package],
        )?;
        tx.execute("DELETE FROM topics WHERE package = ?1", [&record.package])?;
        tx.execute(
            "DELETE FROM vignettes WHERE package = ?1",
            [&record.package],
        )?;
        tx.execute("DELETE FROM files WHERE package = ?1", [&record.package])?;
        tx.execute(
            "DELETE FROM search_documents WHERE package = ?1",
            [&record.package],
        )?;
        tx.execute(
            "DELETE FROM methods_index WHERE package = ?1",
            [&record.package],
        )?;
        tx.execute(
            "DELETE FROM package_links WHERE package = ?1",
            [&record.package],
        )?;
        tx.execute(
            "DELETE FROM call_refs WHERE package = ?1",
            [&record.package],
        )?;

        tx.execute(
            "INSERT INTO package_records
               (package, version, title, install_path, package_json, exports_json, objects_json, signatures_json, indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                record.package,
                record.version,
                record.title,
                record.install_path.display().to_string(),
                serde_json::to_string(&record.package_json)?,
                serde_json::to_string(&record.exports)?,
                serde_json::to_string(&record.objects)?,
                serde_json::to_string(&record.signatures_json)?,
                record.indexed_at,
            ],
        )
        .context("failed to persist indexed package record")?;

        for topic in &record.topics {
            tx.execute(
                "INSERT INTO topics
                   (package, topic, title, aliases_json, description, usage, value, examples, text)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    record.package,
                    topic.topic,
                    topic.title,
                    serde_json::to_string(&topic.aliases)?,
                    topic.description,
                    topic.usage,
                    topic.value,
                    topic.examples,
                    topic.text,
                ],
            )
            .context("failed to persist indexed topic")?;

            let searchable_text = join_searchable_text(&[
                topic.description.as_deref(),
                topic.usage.as_deref(),
                topic.value.as_deref(),
                topic.text.as_deref(),
            ]);
            tx.execute(
                "INSERT INTO search_documents (package, kind, doc_key, title, text)
                 VALUES (?1, 'topic', ?2, ?3, ?4)",
                params![record.package, topic.topic, topic.title, searchable_text,],
            )?;

            if let Some(examples) = &topic.examples
                && !examples.trim().is_empty()
            {
                tx.execute(
                    "INSERT INTO search_documents (package, kind, doc_key, title, text)
                     VALUES (?1, 'example', ?2, ?3, ?4)",
                    params![record.package, topic.topic, topic.title, examples],
                )?;
            }
        }

        for vignette in &record.vignettes {
            tx.execute(
                "INSERT INTO vignettes
                   (package, topic, title, source_path, r_path, pdf_path, text_kind, text)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    record.package,
                    vignette.topic,
                    vignette.title,
                    vignette.source_path,
                    vignette.r_path,
                    vignette.pdf_path,
                    vignette.text_kind,
                    vignette.text,
                ],
            )
            .context("failed to persist indexed vignette")?;

            if let Some(text) = &vignette.text
                && !text.trim().is_empty()
            {
                tx.execute(
                    "INSERT INTO search_documents (package, kind, doc_key, title, text)
                     VALUES (?1, 'vignette', ?2, ?3, ?4)",
                    params![record.package, vignette.topic, vignette.title, text],
                )?;
            }
        }

        for file in &record.files {
            tx.execute(
                "INSERT INTO files
                   (package, path, text_kind, text)
                 VALUES (?1, ?2, ?3, ?4)",
                params![record.package, file.path, file.text_kind, file.text],
            )
            .context("failed to persist indexed file")?;
            tx.execute(
                "INSERT INTO search_documents (package, kind, doc_key, title, text)
                 VALUES (?1, 'file', ?2, ?3, ?4)",
                params![record.package, file.path, file.path, file.text],
            )?;
        }

        for method in derive_indexed_methods(record) {
            tx.execute(
                "INSERT INTO methods_index
                   (package, generic_name, method_name, class_name, system, source_path)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    method.package,
                    method.generic,
                    method.method_name,
                    method.class_name,
                    method.system,
                    method.source_path,
                ],
            )
            .context("failed to persist indexed method")?;
        }

        for link in derive_package_links(record) {
            tx.execute(
                "INSERT INTO package_links
                   (package, related_package, relation)
                 VALUES (?1, ?2, ?3)",
                params![link.package, link.related_package, link.relation],
            )
            .context("failed to persist indexed package link")?;
        }

        for call_ref in derive_call_refs(record) {
            tx.execute(
                "INSERT INTO call_refs
                   (package, file_path, line_number, caller_symbol, callee_package, callee_symbol, relation, snippet)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    call_ref.package,
                    call_ref.file_path,
                    call_ref.line_number as i64,
                    call_ref.caller_symbol,
                    call_ref.callee_package,
                    call_ref.callee_symbol,
                    call_ref.relation,
                    call_ref.snippet,
                ],
            )
            .context("failed to persist indexed call ref")?;
        }

        tx.commit().context("failed to commit index transaction")?;
        Ok(())
    }

    pub fn get_indexed_package_summary(
        &self,
        package: &str,
    ) -> Result<Option<IndexedPackageSummary>> {
        let row = self
            .conn
            .query_row(
                "SELECT package, version, title, install_path, exports_json, objects_json, signatures_json, indexed_at
                 FROM package_records
                 WHERE package = ?1",
                [package],
                |row| {
                    let package: String = row.get(0)?;
                    let version: Option<String> = row.get(1)?;
                    let title: Option<String> = row.get(2)?;
                    let install_path = PathBuf::from(row.get::<_, String>(3)?);
                    let exports: Vec<String> = serde_json::from_str(&row.get::<_, String>(4)?)
                        .unwrap_or_default();
                    let objects: Vec<String> = serde_json::from_str(&row.get::<_, String>(5)?)
                        .unwrap_or_default();
                    let signatures_json: Value =
                        serde_json::from_str(&row.get::<_, String>(6)?).unwrap_or(Value::Null);
                    let signatures_count = signatures_json
                        .as_array()
                        .map(std::vec::Vec::len)
                        .unwrap_or_default();

                    Ok(IndexedPackageSummary {
                        package,
                        version,
                        title,
                        install_path,
                        exports_count: exports.len(),
                        objects_count: objects.len(),
                        signatures_count,
                        topics_count: 0,
                        vignettes_count: 0,
                        files_count: 0,
                        indexed_at: row.get(7)?,
                    })
                },
            )
            .optional()
            .context("failed to load indexed package summary")?;

        let Some(mut summary) = row else {
            return Ok(None);
        };

        summary.topics_count = self.count_where("topics", "package", &summary.package)?;
        summary.vignettes_count = self.count_where("vignettes", "package", &summary.package)?;
        summary.files_count = self.count_where("files", "package", &summary.package)?;
        Ok(Some(summary))
    }

    pub fn get_indexed_package_data(&self, package: &str) -> Result<Option<IndexedPackageData>> {
        self.conn
            .query_row(
                "SELECT package, version, title, install_path, package_json, exports_json, objects_json, signatures_json, indexed_at
                 FROM package_records
                 WHERE package = ?1",
                [package],
                |row| {
                    Ok(IndexedPackageData {
                        package: row.get(0)?,
                        version: row.get(1)?,
                        title: row.get(2)?,
                        install_path: PathBuf::from(row.get::<_, String>(3)?),
                        package_json: serde_json::from_str(&row.get::<_, String>(4)?)
                            .unwrap_or(Value::Null),
                        exports: serde_json::from_str(&row.get::<_, String>(5)?)
                            .unwrap_or_default(),
                        objects: serde_json::from_str(&row.get::<_, String>(6)?)
                            .unwrap_or_default(),
                        signatures_json: serde_json::from_str(&row.get::<_, String>(7)?)
                            .unwrap_or(Value::Null),
                        indexed_at: row.get(8)?,
                    })
                },
            )
            .optional()
            .context("failed to load indexed package data")
    }

    pub fn get_indexed_topics(&self, package: &str) -> Result<Vec<IndexedTopic>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT topic, title, aliases_json, description, usage, value, examples, text
                 FROM topics
                 WHERE package = ?1
                 ORDER BY topic",
            )
            .context("failed to prepare indexed topic query")?;
        let rows = stmt
            .query_map([package], |row| {
                Ok(IndexedTopic {
                    topic: row.get(0)?,
                    title: row.get(1)?,
                    aliases: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    description: row.get(3)?,
                    usage: row.get(4)?,
                    value: row.get(5)?,
                    examples: row.get(6)?,
                    text: row.get(7)?,
                })
            })
            .context("failed to execute indexed topic query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect indexed topics")
    }

    pub fn get_indexed_vignettes(&self, package: &str) -> Result<Vec<IndexedVignette>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT topic, title, source_path, r_path, pdf_path, text_kind, text
                 FROM vignettes
                 WHERE package = ?1
                 ORDER BY topic",
            )
            .context("failed to prepare indexed vignette query")?;
        let rows = stmt
            .query_map([package], |row| {
                Ok(IndexedVignette {
                    topic: row.get(0)?,
                    title: row.get(1)?,
                    source_path: row.get(2)?,
                    r_path: row.get(3)?,
                    pdf_path: row.get(4)?,
                    text_kind: row.get(5)?,
                    text: row.get(6)?,
                })
            })
            .context("failed to execute indexed vignette query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect indexed vignettes")
    }

    pub fn get_indexed_files(&self, package: &str) -> Result<Vec<IndexedFile>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT path, text_kind, text
                 FROM files
                 WHERE package = ?1
                 ORDER BY path",
            )
            .context("failed to prepare indexed file query")?;
        let rows = stmt
            .query_map([package], |row| {
                Ok(IndexedFile {
                    path: row.get(0)?,
                    text_kind: row.get(1)?,
                    text: row.get(2)?,
                })
            })
            .context("failed to execute indexed file query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect indexed files")
    }

    pub fn get_indexed_methods(&self, package: &str) -> Result<Vec<IndexedMethod>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT package, generic_name, method_name, class_name, system, source_path
                 FROM methods_index
                 WHERE package = ?1
                 ORDER BY generic_name, method_name",
            )
            .context("failed to prepare indexed method query")?;
        let rows = stmt
            .query_map([package], |row| {
                Ok(IndexedMethod {
                    package: row.get(0)?,
                    generic: row.get(1)?,
                    method_name: row.get(2)?,
                    class_name: row.get(3)?,
                    system: row.get(4)?,
                    source_path: row.get(5)?,
                })
            })
            .context("failed to execute indexed method query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect indexed methods")
    }

    pub fn find_methods(
        &self,
        generic: &str,
        packages: Option<&[String]>,
    ) -> Result<Vec<IndexedMethod>> {
        let mut methods = if let Some(packages) = packages {
            let mut collected = Vec::new();
            for package in packages {
                collected.extend(self.get_indexed_methods(package)?);
            }
            collected
        } else {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT package, generic_name, method_name, class_name, system, source_path
                     FROM methods_index
                     ORDER BY package, generic_name, method_name",
                )
                .context("failed to prepare cross-package method query")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(IndexedMethod {
                        package: row.get(0)?,
                        generic: row.get(1)?,
                        method_name: row.get(2)?,
                        class_name: row.get(3)?,
                        system: row.get(4)?,
                        source_path: row.get(5)?,
                    })
                })
                .context("failed to execute cross-package method query")?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .context("failed to collect cross-package methods")?
        };

        methods.retain(|method| method.generic == generic);
        Ok(methods)
    }

    pub fn get_package_links(&self, package: &str) -> Result<Vec<IndexedPackageLink>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT package, related_package, relation
                 FROM package_links
                 WHERE package = ?1
                 ORDER BY relation, related_package",
            )
            .context("failed to prepare package-link query")?;
        let rows = stmt
            .query_map([package], |row| {
                Ok(IndexedPackageLink {
                    package: row.get(0)?,
                    related_package: row.get(1)?,
                    relation: row.get(2)?,
                })
            })
            .context("failed to execute package-link query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect package links")
    }

    pub fn indexed_packages(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT package FROM package_records ORDER BY package")
            .context("failed to prepare indexed package query")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .context("failed to execute indexed package query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect indexed package names")
    }

    pub fn get_call_refs(&self, package: &str) -> Result<Vec<IndexedCallRef>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT package, file_path, line_number, caller_symbol, callee_package, callee_symbol, relation, snippet
                 FROM call_refs
                 WHERE package = ?1
                 ORDER BY file_path, line_number, callee_symbol",
            )
            .context("failed to prepare call-ref query")?;
        let rows = stmt
            .query_map([package], |row| {
                Ok(IndexedCallRef {
                    package: row.get(0)?,
                    file_path: row.get(1)?,
                    line_number: row.get::<_, i64>(2)? as usize,
                    caller_symbol: row.get(3)?,
                    callee_package: row.get(4)?,
                    callee_symbol: row.get(5)?,
                    relation: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .context("failed to execute call-ref query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect call refs")
    }

    pub fn find_calls_to_symbol(
        &self,
        target_package: &str,
        target_symbol: &str,
    ) -> Result<Vec<IndexedCallRef>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT package, file_path, line_number, caller_symbol, callee_package, callee_symbol, relation, snippet
                 FROM call_refs
                 WHERE callee_package = ?1 AND callee_symbol = ?2
                 ORDER BY package, file_path, line_number",
            )
            .context("failed to prepare incoming-call query")?;
        let rows = stmt
            .query_map(params![target_package, target_symbol], |row| {
                Ok(IndexedCallRef {
                    package: row.get(0)?,
                    file_path: row.get(1)?,
                    line_number: row.get::<_, i64>(2)? as usize,
                    caller_symbol: row.get(3)?,
                    callee_package: row.get(4)?,
                    callee_symbol: row.get(5)?,
                    relation: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .context("failed to execute incoming-call query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect incoming calls")
    }

    pub fn add_snippet(&mut self, snippet: &NewSnippet) -> Result<IndexedSnippet> {
        let created_at = now_timestamp()?;
        let tx = self
            .conn
            .transaction()
            .context("failed to start snippet transaction")?;
        tx.execute(
            "INSERT INTO snippets
               (title, body, packages_json, objects_json, tags_json, verbs_json, status, source, package_versions_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                snippet.title,
                snippet.body,
                serde_json::to_string(&snippet.packages)?,
                serde_json::to_string(&snippet.objects)?,
                serde_json::to_string(&snippet.tags)?,
                serde_json::to_string(&snippet.verbs)?,
                snippet.status,
                snippet.source,
                serde_json::to_string(&snippet.package_versions)?,
                created_at,
                created_at,
            ],
        )
        .context("failed to persist snippet")?;

        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO search_documents (package, kind, doc_key, title, text)
             VALUES ('', 'snippet', ?1, ?2, ?3)",
            params![id.to_string(), snippet.title, snippet_search_text(snippet),],
        )
        .context("failed to persist snippet search document")?;
        tx.commit()
            .context("failed to commit snippet transaction")?;
        self.get_snippet(id)?
            .ok_or_else(|| anyhow!("snippet disappeared after insert"))
    }

    pub fn get_snippet(&self, id: i64) -> Result<Option<IndexedSnippet>> {
        self.conn
            .query_row(
                "SELECT id, title, body, packages_json, objects_json, tags_json, verbs_json, status, source, package_versions_json, created_at, updated_at
                 FROM snippets
                 WHERE id = ?1",
                [id],
                |row| {
                    Ok(IndexedSnippet {
                        id: row.get(0)?,
                        title: row.get(1)?,
                        body: row.get(2)?,
                        packages: serde_json::from_str(&row.get::<_, String>(3)?)
                            .unwrap_or_default(),
                        objects: serde_json::from_str(&row.get::<_, String>(4)?)
                            .unwrap_or_default(),
                        tags: serde_json::from_str(&row.get::<_, String>(5)?)
                            .unwrap_or_default(),
                        verbs: serde_json::from_str(&row.get::<_, String>(6)?)
                            .unwrap_or_default(),
                        status: row.get(7)?,
                        source: row.get(8)?,
                        package_versions: serde_json::from_str(&row.get::<_, String>(9)?)
                            .unwrap_or_default(),
                        created_at: row.get(10)?,
                        updated_at: row.get(11)?,
                    })
                },
            )
            .optional()
            .context("failed to load snippet")
    }

    pub fn list_snippets(
        &self,
        package: Option<&str>,
        tag: Option<&str>,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IndexedSnippet>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, title, body, packages_json, objects_json, tags_json, verbs_json, status, source, package_versions_json, created_at, updated_at
                 FROM snippets
                 ORDER BY updated_at DESC, id DESC",
            )
            .context("failed to prepare snippet listing query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(IndexedSnippet {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    body: row.get(2)?,
                    packages: serde_json::from_str(&row.get::<_, String>(3)?).unwrap_or_default(),
                    objects: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                    tags: serde_json::from_str(&row.get::<_, String>(5)?).unwrap_or_default(),
                    verbs: serde_json::from_str(&row.get::<_, String>(6)?).unwrap_or_default(),
                    status: row.get(7)?,
                    source: row.get(8)?,
                    package_versions: serde_json::from_str(&row.get::<_, String>(9)?)
                        .unwrap_or_default(),
                    created_at: row.get(10)?,
                    updated_at: row.get(11)?,
                })
            })
            .context("failed to execute snippet listing query")?;

        let mut snippets = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect snippets")?;
        snippets.retain(|snippet| snippet_matches_filters(snippet, package, tag, status));
        snippets.truncate(limit);
        Ok(snippets)
    }

    pub fn search_snippets(
        &self,
        query: &str,
        package: Option<&str>,
        tag: Option<&str>,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IndexSearchMatch>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT doc_key, title, snippet(search_documents, 4, '[', ']', ' … ', 18), bm25(search_documents, 0.0, 0.0, 0.0, 8.0, 1.0)
                 FROM search_documents
                 WHERE kind = 'snippet' AND search_documents MATCH ?1
                 ORDER BY bm25(search_documents, 0.0, 0.0, 0.0, 8.0, 1.0), title, doc_key
                 LIMIT ?2",
            )
            .context("failed to prepare snippet search")?;
        let fetch_limit = std::cmp::max(limit.saturating_mul(10), 50) as i64;
        let rows = stmt
            .query_map(params![query, fetch_limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .context("failed to execute snippet search")?;

        let mut matches = Vec::new();
        for row in rows {
            let (doc_key, title, snippet, score) = row.context("failed to collect snippet row")?;
            let Some(id) = doc_key.parse::<i64>().ok() else {
                continue;
            };
            let Some(snippet_row) = self.get_snippet(id)? else {
                continue;
            };
            if !snippet_matches_filters(&snippet_row, package, tag, status) {
                continue;
            }
            matches.push(IndexSearchMatch {
                kind: "snippet".to_string(),
                key: doc_key,
                title,
                snippet,
                score,
            });
            if matches.len() >= limit {
                break;
            }
        }
        Ok(matches)
    }

    pub fn delete_snippet(&mut self, id: i64) -> Result<bool> {
        let tx = self
            .conn
            .transaction()
            .context("failed to start snippet delete transaction")?;
        tx.execute(
            "DELETE FROM search_documents WHERE kind = 'snippet' AND doc_key = ?1",
            [id.to_string()],
        )
        .context("failed to delete snippet search row")?;
        let deleted = tx
            .execute("DELETE FROM snippets WHERE id = ?1", [id])
            .context("failed to delete snippet")?;
        tx.commit()
            .context("failed to commit snippet delete transaction")?;
        Ok(deleted > 0)
    }

    pub fn refresh_snippet(
        &mut self,
        id: i64,
        package_versions: &BTreeMap<String, String>,
        status: Option<&str>,
    ) -> Result<Option<IndexedSnippet>> {
        let updated_at = now_timestamp()?;
        let tx = self
            .conn
            .transaction()
            .context("failed to start snippet refresh transaction")?;
        let updated = tx
            .execute(
                "UPDATE snippets
                 SET package_versions_json = ?2,
                     status = COALESCE(?3, status),
                     updated_at = ?4
                 WHERE id = ?1",
                params![
                    id,
                    serde_json::to_string(package_versions)?,
                    status,
                    updated_at,
                ],
            )
            .context("failed to refresh snippet")?;
        tx.commit()
            .context("failed to commit snippet refresh transaction")?;
        if updated == 0 {
            return Ok(None);
        }
        self.get_snippet(id)
    }

    pub fn search_package_documents(
        &self,
        package: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<IndexSearchMatch>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT kind, doc_key, title, snippet(search_documents, 4, '[', ']', ' … ', 12), bm25(search_documents, 0.0, 0.0, 0.0, 8.0, 1.0)
                 FROM search_documents
                 WHERE package = ?1 AND search_documents MATCH ?2
                 ORDER BY bm25(search_documents, 0.0, 0.0, 0.0, 8.0, 1.0), title, doc_key
                 LIMIT ?3",
            )
            .context("failed to prepare package index search")?;
        let rows = stmt
            .query_map(params![package, query, limit as i64], |row| {
                Ok(IndexSearchMatch {
                    kind: row.get(0)?,
                    key: row.get(1)?,
                    title: row.get(2)?,
                    snippet: row.get(3)?,
                    score: row.get(4)?,
                })
            })
            .context("failed to execute package index search")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect package index search results")
    }

    pub fn clear(&self) -> Result<usize> {
        let count = self.count("package_index_state")?;
        self.conn
            .execute("DELETE FROM package_index_state", [])
            .context("failed to clear indexed package state")?;
        self.conn
            .execute("DELETE FROM package_records", [])
            .context("failed to clear indexed package records")?;
        self.conn
            .execute("DELETE FROM topics", [])
            .context("failed to clear indexed topics")?;
        self.conn
            .execute("DELETE FROM vignettes", [])
            .context("failed to clear indexed vignettes")?;
        self.conn
            .execute("DELETE FROM files", [])
            .context("failed to clear indexed files")?;
        self.conn
            .execute("DELETE FROM snippets", [])
            .context("failed to clear indexed snippets")?;
        self.conn
            .execute("DELETE FROM search_documents", [])
            .context("failed to clear indexed search docs")?;
        self.conn
            .execute("DELETE FROM methods_index", [])
            .context("failed to clear indexed methods")?;
        self.conn
            .execute("DELETE FROM package_links", [])
            .context("failed to clear indexed package links")?;
        self.conn
            .execute("DELETE FROM call_refs", [])
            .context("failed to clear indexed call refs")?;
        Ok(count)
    }

    fn ensure_schema(&mut self) -> Result<()> {
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS metadata (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS package_index_state (
                  package TEXT PRIMARY KEY,
                  install_path TEXT NOT NULL,
                  helper_fingerprint TEXT,
                  local_fingerprint TEXT NOT NULL,
                  updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS package_records (
                  package TEXT PRIMARY KEY,
                  version TEXT,
                  title TEXT,
                  install_path TEXT NOT NULL,
                  package_json TEXT NOT NULL,
                  exports_json TEXT NOT NULL,
                  objects_json TEXT NOT NULL,
                  signatures_json TEXT NOT NULL,
                  indexed_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS topics (
                  package TEXT NOT NULL,
                  topic TEXT NOT NULL,
                  title TEXT,
                  aliases_json TEXT NOT NULL,
                  description TEXT,
                  usage TEXT,
                  value TEXT,
                  examples TEXT,
                  text TEXT,
                  PRIMARY KEY(package, topic)
                );

                CREATE TABLE IF NOT EXISTS vignettes (
                  package TEXT NOT NULL,
                  topic TEXT NOT NULL,
                  title TEXT,
                  source_path TEXT,
                  r_path TEXT,
                  pdf_path TEXT,
                  text_kind TEXT,
                  text TEXT,
                  PRIMARY KEY(package, topic)
                );

                CREATE TABLE IF NOT EXISTS files (
                  package TEXT NOT NULL,
                  path TEXT NOT NULL,
                  text_kind TEXT NOT NULL,
                  text TEXT NOT NULL,
                  PRIMARY KEY(package, path)
                );

                CREATE TABLE IF NOT EXISTS snippets (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  title TEXT NOT NULL,
                  body TEXT NOT NULL,
                  packages_json TEXT NOT NULL,
                  objects_json TEXT NOT NULL,
                  tags_json TEXT NOT NULL,
                  verbs_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  source TEXT,
                  package_versions_json TEXT NOT NULL,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS methods_index (
                  package TEXT NOT NULL,
                  generic_name TEXT NOT NULL,
                  method_name TEXT NOT NULL,
                  class_name TEXT,
                  system TEXT NOT NULL,
                  source_path TEXT,
                  PRIMARY KEY(package, method_name, system, source_path)
                );

                CREATE TABLE IF NOT EXISTS package_links (
                  package TEXT NOT NULL,
                  related_package TEXT NOT NULL,
                  relation TEXT NOT NULL,
                  PRIMARY KEY(package, related_package, relation)
                );

                CREATE TABLE IF NOT EXISTS call_refs (
                  package TEXT NOT NULL,
                  file_path TEXT NOT NULL,
                  line_number INTEGER NOT NULL,
                  caller_symbol TEXT,
                  callee_package TEXT,
                  callee_symbol TEXT NOT NULL,
                  relation TEXT NOT NULL,
                  snippet TEXT NOT NULL
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS search_documents USING fts5(
                  package UNINDEXED,
                  kind UNINDEXED,
                  doc_key UNINDEXED,
                  title,
                  text
                );
                ",
            )
            .context("failed to initialize index schema")?;

        let version = self.schema_version_optional()?;
        match version {
            Some(version) if version == INDEX_SCHEMA_VERSION => Ok(()),
            Some(version) => {
                bail!("unsupported index schema version {version}; expected {INDEX_SCHEMA_VERSION}")
            }
            None => {
                self.conn
                    .execute(
                        "INSERT INTO metadata (key, value) VALUES ('schema_version', ?1)",
                        [INDEX_SCHEMA_VERSION.to_string()],
                    )
                    .context("failed to record index schema version")?;
                Ok(())
            }
        }
    }

    fn schema_version(&self) -> Result<i64> {
        self.schema_version_optional()?
            .ok_or_else(|| anyhow!("index schema version is missing"))
    }

    fn schema_version_optional(&self) -> Result<Option<i64>> {
        let raw = self
            .conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed to query index schema version")?;
        raw.map(|value| {
            value
                .parse::<i64>()
                .with_context(|| format!("invalid index schema version value '{value}'"))
        })
        .transpose()
    }

    fn count(&self, table: &str) -> Result<usize> {
        let query = format!("SELECT COUNT(*) FROM {table}");
        let count: i64 = self
            .conn
            .query_row(&query, [], |row| row.get(0))
            .with_context(|| format!("failed to count rows from {table}"))?;
        Ok(count as usize)
    }

    fn count_where(&self, table: &str, column: &str, value: &str) -> Result<usize> {
        let query = format!("SELECT COUNT(*) FROM {table} WHERE {column} = ?1");
        let count: i64 = self
            .conn
            .query_row(&query, [value], |row| row.get(0))
            .with_context(|| format!("failed to count rows from {table}"))?;
        Ok(count as usize)
    }
}

fn join_searchable_text(parts: &[Option<&str>]) -> String {
    parts
        .iter()
        .flatten()
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn snippet_search_text(snippet: &NewSnippet) -> String {
    let mut chunks = vec![snippet.body.trim().to_string()];
    if !snippet.packages.is_empty() {
        chunks.push(snippet.packages.join(" "));
    }
    if !snippet.objects.is_empty() {
        chunks.push(snippet.objects.join(" "));
    }
    if !snippet.tags.is_empty() {
        chunks.push(snippet.tags.join(" "));
    }
    if !snippet.verbs.is_empty() {
        chunks.push(snippet.verbs.join(" "));
    }
    chunks
        .into_iter()
        .filter(|chunk| !chunk.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn snippet_matches_filters(
    snippet: &IndexedSnippet,
    package: Option<&str>,
    tag: Option<&str>,
    status: Option<&str>,
) -> bool {
    if let Some(package) = package
        && !snippet.packages.iter().any(|entry| entry == package)
    {
        return false;
    }
    if let Some(tag) = tag
        && !snippet.tags.iter().any(|entry| entry == tag)
    {
        return false;
    }
    if let Some(status) = status
        && snippet.status != status
    {
        return false;
    }
    true
}

fn derive_indexed_methods(record: &IndexedPackageRecord) -> Vec<IndexedMethod> {
    let mut methods = Vec::new();

    for object_name in &record.objects {
        if let Some((generic, class_name)) = parse_s3_method_name(object_name) {
            methods.push(IndexedMethod {
                package: record.package.clone(),
                generic,
                method_name: object_name.clone(),
                class_name: Some(class_name),
                system: "s3".to_string(),
                source_path: None,
            });
        }
    }

    for file in &record.files {
        for (generic, class_name) in parse_s4_methods_from_text(&file.text) {
            methods.push(IndexedMethod {
                package: record.package.clone(),
                generic: generic.clone(),
                method_name: format!("{generic}.{class_name}"),
                class_name: Some(class_name),
                system: "s4".to_string(),
                source_path: Some(file.path.clone()),
            });
        }
    }

    methods.sort_by(|left, right| {
        (
            left.generic.as_str(),
            left.method_name.as_str(),
            left.system.as_str(),
        )
            .cmp(&(
                right.generic.as_str(),
                right.method_name.as_str(),
                right.system.as_str(),
            ))
    });
    methods.dedup();
    methods
}

fn parse_s3_method_name(name: &str) -> Option<(String, String)> {
    let trimmed = name.trim();
    if trimmed.starts_with('.') || !trimmed.contains('.') {
        return None;
    }
    let (generic, class_name) = trimmed.split_once('.')?;
    if generic.is_empty() || class_name.is_empty() {
        return None;
    }
    Some((generic.to_string(), class_name.to_string()))
}

fn parse_s4_methods_from_text(text: &str) -> Vec<(String, String)> {
    let mut methods = Vec::new();
    for line in text.lines() {
        if !line.contains("setMethod(") {
            continue;
        }
        let quoted = line.split('"').collect::<Vec<_>>();
        if quoted.len() >= 4 {
            let generic = quoted[1].trim();
            let class_name = quoted[3].trim();
            if !generic.is_empty() && !class_name.is_empty() {
                methods.push((generic.to_string(), class_name.to_string()));
            }
        }
    }
    methods
}

fn derive_package_links(record: &IndexedPackageRecord) -> Vec<IndexedPackageLink> {
    let mut links = Vec::new();
    for (relation, key) in [
        ("depends", "depends"),
        ("imports", "imports"),
        ("suggests", "suggests"),
        ("linking_to", "linking_to"),
    ] {
        let packages = record
            .package_json
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter_map(normalize_dependency_name);
        for related_package in packages {
            links.push(IndexedPackageLink {
                package: record.package.clone(),
                related_package,
                relation: relation.to_string(),
            });
        }
    }
    links.sort_by(|left, right| {
        (
            left.relation.as_str(),
            left.related_package.as_str(),
            left.package.as_str(),
        )
            .cmp(&(
                right.relation.as_str(),
                right.related_package.as_str(),
                right.package.as_str(),
            ))
    });
    links.dedup();
    links
}

fn derive_call_refs(record: &IndexedPackageRecord) -> Vec<IndexedCallRef> {
    let mut refs = Vec::new();
    let namespace_text = record
        .files
        .iter()
        .find(|file| file.path.eq_ignore_ascii_case("NAMESPACE"))
        .map(|file| file.text.as_str())
        .unwrap_or_default();
    let imported_symbols = parse_namespace_imports(namespace_text);

    for (target_package, symbols) in &imported_symbols {
        for symbol in symbols {
            refs.push(IndexedCallRef {
                package: record.package.clone(),
                file_path: "NAMESPACE".to_string(),
                line_number: 0,
                caller_symbol: None,
                callee_package: Some(target_package.clone()),
                callee_symbol: symbol.clone(),
                relation: "namespace_import".to_string(),
                snippet: format!("importFrom({target_package}, {symbol})"),
            });
        }
    }

    for file in &record.files {
        refs.extend(derive_call_refs_from_file(
            &record.package,
            file,
            &imported_symbols,
        ));
    }

    refs.sort_by(|left, right| {
        (
            left.package.as_str(),
            left.file_path.as_str(),
            left.line_number,
            left.callee_package.as_deref().unwrap_or_default(),
            left.callee_symbol.as_str(),
            left.relation.as_str(),
        )
            .cmp(&(
                right.package.as_str(),
                right.file_path.as_str(),
                right.line_number,
                right.callee_package.as_deref().unwrap_or_default(),
                right.callee_symbol.as_str(),
                right.relation.as_str(),
            ))
    });
    refs.dedup();
    refs
}

fn parse_namespace_imports(text: &str) -> std::collections::BTreeMap<String, Vec<String>> {
    let mut imports =
        std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();

    for line in text.lines() {
        let compact = line.replace(' ', "");
        if let Some(rest) = compact.strip_prefix("importFrom(")
            && let Some(end) = rest.find(')')
        {
            let inner = &rest[..end];
            let mut parts = inner.split(',');
            if let Some(package) = parts.next() {
                let entry = imports.entry(package.to_string()).or_default();
                for symbol in parts {
                    if !symbol.is_empty() {
                        entry.insert(symbol.to_string());
                    }
                }
            }
        }

        if let Some(rest) = line.trim().strip_prefix("@importFrom") {
            let mut parts = rest.split_whitespace();
            if let Some(package) = parts.next() {
                let entry = imports.entry(package.to_string()).or_default();
                for symbol in parts {
                    let normalized = symbol.trim_matches(',').trim();
                    if !normalized.is_empty() {
                        entry.insert(normalized.to_string());
                    }
                }
            }
        }
    }

    imports
        .into_iter()
        .map(|(package, symbols)| (package, symbols.into_iter().collect()))
        .collect()
}

fn derive_call_refs_from_file(
    package: &str,
    file: &IndexedFile,
    imported_symbols: &std::collections::BTreeMap<String, Vec<String>>,
) -> Vec<IndexedCallRef> {
    let mut refs = Vec::new();
    let mut current_caller: Option<String> = None;
    let mut current_depth: isize = 0;

    for (line_idx, line) in file.text.lines().enumerate() {
        if let Some((symbol, depth_delta)) = parse_function_definition(line) {
            current_caller = Some(symbol);
            current_depth = depth_delta;
        } else if current_caller.is_some() {
            current_depth += brace_delta(line);
            if current_depth <= 0 {
                current_caller = None;
                current_depth = 0;
            }
        }

        refs.extend(extract_qualified_refs(
            package,
            file,
            line,
            line_idx + 1,
            current_caller.clone(),
        ));
        refs.extend(extract_imported_symbol_refs(
            package,
            file,
            line,
            line_idx + 1,
            current_caller.clone(),
            imported_symbols,
        ));
    }

    refs
}

fn parse_function_definition(line: &str) -> Option<(String, isize)> {
    let trimmed = line.trim_start();
    let assignment = if let Some(idx) = trimmed.find("<- function") {
        idx
    } else {
        trimmed.find("= function")?
    };
    let candidate = trimmed[..assignment].trim();
    if candidate.is_empty()
        || !candidate
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.'))
    {
        return None;
    }
    Some((candidate.to_string(), brace_delta(trimmed)))
}

fn brace_delta(line: &str) -> isize {
    line.chars().fold(0, |acc, ch| match ch {
        '{' => acc + 1,
        '}' => acc - 1,
        _ => acc,
    })
}

fn extract_qualified_refs(
    package: &str,
    file: &IndexedFile,
    line: &str,
    line_number: usize,
    caller_symbol: Option<String>,
) -> Vec<IndexedCallRef> {
    let mut refs = Vec::new();
    for delimiter in [":::", "::"] {
        let mut search_start = 0usize;
        while let Some(relative_idx) = line[search_start..].find(delimiter) {
            let idx = search_start + relative_idx;
            let package_start = line[..idx]
                .rfind(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .map(|pos| pos + 1)
                .unwrap_or(0);
            let target_package = line[package_start..idx].trim();
            let symbol_start = idx + delimiter.len();
            let target_symbol = line[symbol_start..]
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.'))
                .collect::<String>();
            if !target_package.is_empty() && !target_symbol.is_empty() {
                let remainder = &line[symbol_start + target_symbol.len()..];
                let relation = if remainder.trim_start().starts_with('(') {
                    "qualified_call"
                } else {
                    "qualified_ref"
                };
                refs.push(IndexedCallRef {
                    package: package.to_string(),
                    file_path: file.path.clone(),
                    line_number,
                    caller_symbol: caller_symbol.clone(),
                    callee_package: Some(target_package.to_string()),
                    callee_symbol: target_symbol,
                    relation: relation.to_string(),
                    snippet: line.trim().to_string(),
                });
            }
            search_start = symbol_start;
            if search_start >= line.len() {
                break;
            }
        }
    }
    refs
}

fn extract_imported_symbol_refs(
    package: &str,
    file: &IndexedFile,
    line: &str,
    line_number: usize,
    caller_symbol: Option<String>,
    imported_symbols: &std::collections::BTreeMap<String, Vec<String>>,
) -> Vec<IndexedCallRef> {
    let mut refs = Vec::new();
    for (target_package, symbols) in imported_symbols {
        for symbol in symbols {
            let pattern = format!("{symbol}(");
            if line.contains("::") || !line.contains(&pattern) {
                continue;
            }
            refs.push(IndexedCallRef {
                package: package.to_string(),
                file_path: file.path.clone(),
                line_number,
                caller_symbol: caller_symbol.clone(),
                callee_package: Some(target_package.clone()),
                callee_symbol: symbol.clone(),
                relation: "imported_call".to_string(),
                snippet: line.trim().to_string(),
            });
        }
    }
    refs
}

fn normalize_dependency_name(raw: &str) -> Option<String> {
    let normalized = raw
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches(',')
        .trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

pub fn default_index_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("RPEEK_INDEX_PATH") {
        return Ok(PathBuf::from(path));
    }

    if let Ok(cache_home) = env::var("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(cache_home)
            .join("rpeek")
            .join("index.sqlite3"));
    }

    if let Ok(home) = env::var("HOME") {
        return Ok(PathBuf::from(home)
            .join(".cache")
            .join("rpeek")
            .join("index.sqlite3"));
    }

    Ok(env::temp_dir().join("rpeek").join("index.sqlite3"))
}

pub fn now_timestamp() -> Result<i64> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    Ok(value as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_record() -> IndexedPackageRecord {
        IndexedPackageRecord {
            package: "stats".to_string(),
            version: Some("1.0.0".to_string()),
            title: Some("Stats".to_string()),
            install_path: PathBuf::from("/tmp/stats"),
            package_json: serde_json::json!({
                "package":"stats",
                "version":"1.0.0",
                "suggests":["MASS"],
                "imports":["graphics"]
            }),
            exports: vec!["lm".to_string()],
            objects: vec!["lm".to_string(), "glm".to_string(), "lm.class".to_string()],
            signatures_json: serde_json::json!([
                {"name":"lm","signature":"function (formula, data) NULL"}
            ]),
            topics: vec![IndexedTopic {
                topic: "lm".to_string(),
                title: Some("Linear models".to_string()),
                aliases: vec!["lm".to_string()],
                description: Some("Fit linear models".to_string()),
                usage: None,
                value: None,
                examples: Some("lm(y ~ x)".to_string()),
                text: Some("Detailed docs".to_string()),
            }],
            vignettes: vec![IndexedVignette {
                topic: "reshape".to_string(),
                title: Some("Using reshape".to_string()),
                source_path: Some("/tmp/stats/doc/reshape.Rnw".to_string()),
                r_path: None,
                pdf_path: None,
                text_kind: Some("rnw".to_string()),
                text: Some("reshape long wide".to_string()),
            }],
            files: vec![
                IndexedFile {
                    path: "NAMESPACE".to_string(),
                    text_kind: "text".to_string(),
                    text: "importFrom(graphics, plot)".to_string(),
                },
                IndexedFile {
                    path: "R/example.R".to_string(),
                    text_kind: "r".to_string(),
                    text: "fit_model <- function(x, y) {\n  stats::lm(y ~ x)\n  plot(x, y)\n}\n"
                        .to_string(),
                },
            ],
            indexed_at: 42,
        }
    }

    #[test]
    fn store_initializes_and_reports_stats() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("index.sqlite3");
        let store = IndexStore::open(&db_path).expect("open store");
        let stats = store.stats().expect("stats");

        assert_eq!(stats.schema_version, INDEX_SCHEMA_VERSION);
        assert_eq!(stats.package_count, 0);
        assert_eq!(stats.indexed_packages, 0);
        assert_eq!(stats.path, db_path);
    }

    #[test]
    fn package_state_round_trips() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("index.sqlite3");
        let store = IndexStore::open(&db_path).expect("open store");
        let state = PackageIndexState {
            package: "stats".to_string(),
            install_path: PathBuf::from("/tmp/stats"),
            helper_fingerprint: Some("stats@1.0.0".to_string()),
            local_fingerprint: "fp-1".to_string(),
            updated_at: 42,
        };

        store
            .upsert_package_state(&state)
            .expect("persist package state");
        let loaded = store
            .get_package_state("stats")
            .expect("query package state")
            .expect("missing package state");

        assert_eq!(loaded, state);
    }

    #[test]
    fn indexed_package_round_trips_and_searches() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("index.sqlite3");
        let mut store = IndexStore::open(&db_path).expect("open store");
        let record = sample_record();

        store
            .upsert_package_record(&record)
            .expect("persist package record");
        let summary = store
            .get_indexed_package_summary("stats")
            .expect("query summary")
            .expect("missing summary");
        assert_eq!(summary.package, "stats");
        assert_eq!(summary.exports_count, 1);
        assert_eq!(summary.topics_count, 1);
        assert_eq!(summary.vignettes_count, 1);
        assert_eq!(summary.files_count, 2);

        let matches = store
            .search_package_documents("stats", "reshape", 10)
            .expect("search index");
        assert!(!matches.is_empty());
        assert!(matches.iter().any(|entry| entry.kind == "vignette"));

        let methods = store
            .get_indexed_methods("stats")
            .expect("query indexed methods");
        assert!(methods.iter().any(|entry| entry.method_name == "lm.class"));

        let links = store
            .get_package_links("stats")
            .expect("query package links");
        assert!(links.iter().any(|entry| entry.related_package == "MASS"));

        let call_refs = store.get_call_refs("stats").expect("query call refs");
        assert!(call_refs.iter().any(|entry| {
            entry.relation == "qualified_call"
                && entry.callee_package.as_deref() == Some("stats")
                && entry.callee_symbol == "lm"
        }));
        assert!(call_refs.iter().any(|entry| {
            entry.relation == "imported_call"
                && entry.callee_package.as_deref() == Some("graphics")
                && entry.callee_symbol == "plot"
        }));
    }

    #[test]
    fn clear_removes_package_rows() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("index.sqlite3");
        let mut store = IndexStore::open(&db_path).expect("open store");
        store
            .upsert_package_record(&sample_record())
            .expect("persist package record");
        let state = PackageIndexState {
            package: "stats".to_string(),
            install_path: PathBuf::from("/tmp/stats"),
            helper_fingerprint: None,
            local_fingerprint: "fp-1".to_string(),
            updated_at: 42,
        };

        store
            .upsert_package_state(&state)
            .expect("persist package state");
        assert_eq!(store.clear().expect("clear index"), 1);
        assert!(
            store
                .get_package_state("stats")
                .expect("query package state")
                .is_none()
        );
        assert!(
            store
                .get_indexed_package_summary("stats")
                .expect("query package summary")
                .is_none()
        );
    }

    #[test]
    fn snippets_round_trip_and_rank_title_hits_first() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("index.sqlite3");
        let mut store = IndexStore::open(&db_path).expect("open store");

        let first = store
            .add_snippet(&NewSnippet {
                title: "BIDS workflow".to_string(),
                body: "Use bidser to locate scans, then read them with neuroim2.".to_string(),
                packages: vec!["bidser".to_string(), "neuroim2".to_string()],
                objects: vec!["read_bids_project".to_string()],
                tags: vec!["workflow".to_string()],
                verbs: vec!["read".to_string()],
                status: "verified".to_string(),
                source: Some("notes".to_string()),
                package_versions: BTreeMap::from([
                    ("bidser".to_string(), "0.1.0".to_string()),
                    ("neuroim2".to_string(), "0.6.0".to_string()),
                ]),
            })
            .expect("persist first snippet");
        let second = store
            .add_snippet(&NewSnippet {
                title: "Notes".to_string(),
                body: "This workflow is mostly about loading BIDS-derived volumes.".to_string(),
                packages: vec!["bidser".to_string()],
                objects: vec![],
                tags: vec!["workflow".to_string()],
                verbs: vec!["load".to_string()],
                status: "unknown".to_string(),
                source: None,
                package_versions: BTreeMap::new(),
            })
            .expect("persist second snippet");

        let listed = store
            .list_snippets(Some("bidser"), Some("workflow"), None, 10)
            .expect("list snippets");
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|snippet| snippet.id == first.id));

        let matches = store
            .search_snippets("workflow", Some("bidser"), Some("workflow"), None, 10)
            .expect("search snippets");
        assert_eq!(
            matches.first().map(|entry| entry.key.clone()),
            Some(first.id.to_string())
        );
        assert!(
            matches
                .iter()
                .any(|entry| entry.key == second.id.to_string())
        );

        assert!(store.delete_snippet(first.id).expect("delete snippet"));
        assert!(store.get_snippet(first.id).expect("load deleted").is_none());
    }

    #[test]
    fn refresh_snippet_updates_versions_and_status() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("index.sqlite3");
        let mut store = IndexStore::open(&db_path).expect("open store");

        let snippet = store
            .add_snippet(&NewSnippet {
                title: "Stats workflow".to_string(),
                body: "Call lm.".to_string(),
                packages: vec!["stats".to_string()],
                objects: vec![],
                tags: vec!["workflow".to_string()],
                verbs: vec!["fit".to_string()],
                status: "stale".to_string(),
                source: None,
                package_versions: BTreeMap::from([("stats".to_string(), "1.0.0".to_string())]),
            })
            .expect("persist snippet");

        let refreshed = store
            .refresh_snippet(
                snippet.id,
                &BTreeMap::from([("stats".to_string(), "2.0.0".to_string())]),
                Some("verified"),
            )
            .expect("refresh snippet")
            .expect("missing refreshed snippet");

        assert_eq!(refreshed.status, "verified");
        assert_eq!(
            refreshed.package_versions.get("stats").map(String::as_str),
            Some("2.0.0")
        );
    }
}
