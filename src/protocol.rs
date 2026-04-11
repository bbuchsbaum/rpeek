//! Request types for the Rust client/daemon and R helper protocol.
//!
//! `rpeek` speaks JSON Lines to a long-lived R helper process. This module keeps the
//! contract typed on the Rust side so command dispatch, batch execution, and cache keys
//! all share one representation.
//!
use serde::{Deserialize, Serialize};

/// A single protocol request sent to the helper or daemon.
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Request {
    /// Healthcheck used by the Rust daemon and helper startup logic.
    Ping,
    /// Resolve an installed package path/version for cache invalidation.
    Fingerprint {
        /// Installed R package name.
        package: String,
    },
    /// Return package metadata and install location.
    Pkg {
        /// Installed R package name.
        package: String,
    },
    /// Return exported objects.
    Exports {
        /// Installed R package name.
        package: String,
    },
    /// Return all namespace objects.
    Objects {
        /// Installed R package name.
        package: String,
    },
    /// Search one package's objects and help topics.
    Search {
        /// Installed R package name.
        package: String,
        /// Search query.
        query: String,
        /// Search scope: `all`, `object`, or `topic`.
        kind: String,
        /// Maximum matches to return.
        limit: usize,
    },
    /// Search exports/help topics across installed packages.
    SearchAll {
        /// Search query.
        query: String,
        /// Search scope: `all`, `object`, or `topic`.
        kind: String,
        /// Maximum matches to return.
        limit: usize,
    },
    /// Resolve likely object/topic candidates for a query.
    Resolve {
        /// Search query.
        query: String,
        /// Optional package restriction.
        #[serde(skip_serializing_if = "Option::is_none")]
        package: Option<String>,
        /// Search scope: `all`, `object`, or `topic`.
        kind: String,
        /// Maximum candidates to return.
        limit: usize,
    },
    /// Return a compact summary for one object.
    Summary {
        /// Installed R package name.
        package: String,
        /// Object name.
        name: String,
    },
    /// Return object signature and metadata.
    Sig {
        /// Installed R package name.
        package: String,
        /// Object name.
        name: String,
    },
    /// Return best-effort source for one object.
    Source {
        /// Installed R package name.
        package: String,
        /// Object name.
        name: String,
    },
    /// Return installed help text and structured doc fields.
    Doc {
        /// Installed R package name.
        package: String,
        /// Help topic name.
        topic: String,
    },
    /// Return related S3/S4 methods.
    Methods {
        /// Installed R package name.
        package: String,
        /// Generic/object name.
        name: String,
    },
    /// Return installed package files.
    Files {
        /// Installed R package name.
        package: String,
    },
    /// Search installed package files for a literal query.
    Grep {
        /// Installed R package name.
        package: String,
        /// Literal query string.
        query: String,
        /// Optional glob restriction for relative package paths.
        #[serde(skip_serializing_if = "Option::is_none")]
        glob: Option<String>,
        /// Maximum matches to return.
        limit: usize,
    },
    /// Clear the daemon-local response cache.
    CacheClear,
    /// Return cache statistics.
    CacheStats,
    /// Return daemon status and helper/cache health.
    DaemonStatus,
    /// Stop the daemon.
    Shutdown,
}

impl Request {
    /// Stable action name used in CLI responses and batch handling.
    pub fn action(&self) -> &'static str {
        match self {
            Self::Ping => "ping",
            Self::Fingerprint { .. } => "fingerprint",
            Self::Pkg { .. } => "pkg",
            Self::Exports { .. } => "exports",
            Self::Objects { .. } => "objects",
            Self::Search { .. } => "search",
            Self::SearchAll { .. } => "search_all",
            Self::Resolve { .. } => "resolve",
            Self::Summary { .. } => "summary",
            Self::Sig { .. } => "sig",
            Self::Source { .. } => "source",
            Self::Doc { .. } => "doc",
            Self::Methods { .. } => "methods",
            Self::Files { .. } => "files",
            Self::Grep { .. } => "grep",
            Self::CacheClear => "cache_clear",
            Self::CacheStats => "cache_stats",
            Self::DaemonStatus => "daemon_status",
            Self::Shutdown => "shutdown",
        }
    }

    /// Return the package associated with a request when one exists.
    pub fn package(&self) -> Option<&str> {
        match self {
            Self::Fingerprint { package }
            | Self::Pkg { package }
            | Self::Exports { package }
            | Self::Objects { package }
            | Self::Search { package, .. }
            | Self::Summary { package, .. }
            | Self::Sig { package, .. }
            | Self::Source { package, .. }
            | Self::Doc { package, .. }
            | Self::Methods { package, .. }
            | Self::Files { package }
            | Self::Grep { package, .. } => Some(package),
            Self::Resolve { package, .. } => package.as_deref(),
            _ => None,
        }
    }

    /// Whether this request can run directly against a one-shot helper process.
    pub fn can_run_without_daemon(&self) -> bool {
        !matches!(
            self,
            Self::CacheClear | Self::CacheStats | Self::DaemonStatus | Self::Shutdown
        )
    }

    /// Whether this request requires a package to be present.
    pub fn requires_package(&self) -> bool {
        !matches!(
            self,
            Self::Ping
                | Self::SearchAll { .. }
                | Self::Resolve { package: None, .. }
                | Self::CacheClear
                | Self::CacheStats
                | Self::DaemonStatus
                | Self::Shutdown
        )
    }

    /// Whether successful responses for this request should be cached by the daemon.
    pub fn is_cacheable(&self) -> bool {
        !matches!(
            self,
            Self::Ping
                | Self::Shutdown
                | Self::CacheClear
                | Self::CacheStats
                | Self::DaemonStatus
                | Self::Fingerprint { .. }
                | Self::SearchAll { .. }
                | Self::Resolve { package: None, .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_batch_request_shape() {
        let request: Request =
            serde_json::from_str(r#"{"action":"summary","package":"stats","name":"lm"}"#)
                .expect("request should deserialize");

        assert_eq!(request.action(), "summary");
        assert_eq!(request.package(), Some("stats"));
        assert!(request.is_cacheable());
    }

    #[test]
    fn resolve_without_package_does_not_require_package() {
        let request = Request::Resolve {
            query: "lm".to_string(),
            package: None,
            kind: "all".to_string(),
            limit: 10,
        };

        assert!(!request.requires_package());
        assert!(!request.is_cacheable());
    }
}
