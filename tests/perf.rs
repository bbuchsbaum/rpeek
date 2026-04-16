use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;
use tempfile::TempDir;

struct DaemonGuard {
    socket: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_rpeek"))
            .arg("shutdown")
            .env("RPEEK_SOCKET", &self.socket)
            .output();
    }
}

struct Scenario {
    name: &'static str,
    args: &'static [&'static str],
    warmups: usize,
    runs: usize,
    target_median_ms: u128,
    target_p95_ms: u128,
}

#[test]
#[ignore = "manual perf benchmark; run with cargo test --release --test perf -- --ignored --nocapture"]
fn warm_path_benchmarks_report() {
    let tempdir = TempDir::new().expect("failed to create tempdir");
    let socket = tempdir.path().join("rpeek-perf.sock");
    let index_path = tempdir.path().join("rpeek-perf.sqlite3");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let scenarios = [
        Scenario {
            name: "map_stats",
            args: &["map", "stats"],
            warmups: 2,
            runs: 7,
            target_median_ms: 300,
            target_p95_ms: 500,
        },
        Scenario {
            name: "search_stats_topics",
            args: &["search", "--kind", "topic", "--limit", "5", "stats", "lm"],
            warmups: 2,
            runs: 7,
            target_median_ms: 150,
            target_p95_ms: 300,
        },
        Scenario {
            name: "bridge_stats_graphics",
            args: &["bridge", "stats", "graphics"],
            warmups: 2,
            runs: 7,
            target_median_ms: 300,
            target_p95_ms: 550,
        },
    ];

    let mut summaries = Vec::new();
    let enforce = std::env::var("RPEEK_ENFORCE_BENCH_TARGETS").ok().as_deref() == Some("1");

    for scenario in scenarios {
        for _ in 0..scenario.warmups {
            let _ = run_with_socket(&socket, &index_path, scenario.args);
        }

        let mut samples = Vec::new();
        for _ in 0..scenario.runs {
            let duration_ms = run_with_socket(&socket, &index_path, scenario.args);
            samples.push(duration_ms);
        }

        let median_ms = percentile_ms(&samples, 0.50);
        let p95_ms = percentile_ms(&samples, 0.95);

        if enforce {
            assert!(
                median_ms <= scenario.target_median_ms,
                "{} median {}ms exceeded target {}ms",
                scenario.name,
                median_ms,
                scenario.target_median_ms
            );
            assert!(
                p95_ms <= scenario.target_p95_ms,
                "{} p95 {}ms exceeded target {}ms",
                scenario.name,
                p95_ms,
                scenario.target_p95_ms
            );
        }

        summaries.push(serde_json::json!({
            "name": scenario.name,
            "args": scenario.args,
            "samples_ms": samples,
            "median_ms": median_ms,
            "p95_ms": p95_ms,
            "targets": {
                "median_ms": scenario.target_median_ms,
                "p95_ms": scenario.target_p95_ms,
            },
            "status": if median_ms <= scenario.target_median_ms && p95_ms <= scenario.target_p95_ms {
                "within_target"
            } else {
                "over_target"
            }
        }));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "mode": if cfg!(debug_assertions) { "debug" } else { "release" },
            "enforced": enforce,
            "socket": socket.display().to_string(),
            "index_path": index_path.display().to_string(),
            "scenarios": summaries,
        }))
        .expect("failed to encode perf summary")
    );
}

fn run_with_socket(socket: &Path, index_path: &Path, args: &[&str]) -> u128 {
    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_rpeek"))
        .args(args)
        .env("RPEEK_SOCKET", socket)
        .env("RPEEK_INDEX_PATH", index_path)
        .output()
        .expect("failed to run rpeek benchmark scenario");
    let elapsed = started.elapsed().as_millis();

    assert!(
        output.status.success(),
        "scenario {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stdout)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf8");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json");
    assert_eq!(value["ok"], true, "stdout: {stdout}");

    elapsed
}

fn percentile_ms(samples: &[u128], fraction: f64) -> u128 {
    assert!(!samples.is_empty(), "samples must not be empty");
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * fraction).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}
