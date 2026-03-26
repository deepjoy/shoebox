//! Benchmark the BFS per-directory L1 scan (`scan_l1_dir`) against a real
//! directory tree, mimicking what the taskmill executor does but without the
//! server overhead.
//!
//! Runs two passes: cold (empty catalog → all inserts) and warm (full catalog
//! → all unchanged). Prints per-directory and aggregate timing so the DB /
//! syscall bottleneck is visible.
//!
//! Usage:
//!   cargo run --release --example bench_l1_bfs -- /path/to/bucket
//!
//!   # Generate 19k-file test tree first:
//!   python3 examples/gen_test_data.py /tmp/shoebox-bench
//!   cargo run --release --example bench_l1_bfs -- /tmp/shoebox-bench

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Instant;

use shoebox::metadata::MetadataStore;
use shoebox::scanner::levels::scan_l1_dir;
use shoebox::scanner::scope::ScanScope;

/// BFS over all directories starting at `root`, calling `scan_l1_dir` for each,
/// then committing all write ops in a single batch per directory (simulating the
/// single-writer pattern used by the production taskmill executor).
///
/// Returns (dirs_scanned, files_new, files_unchanged, total_elapsed_secs).
async fn run_bfs(
    metadata: &MetadataStore,
    root: &std::path::Path,
    label: &str,
) -> (u64, u64, u64, f64) {
    let t0 = Instant::now();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(String::new()); // root dir_prefix = ""

    let mut dirs = 0u64;
    let mut new_files = 0u64;
    let mut unchanged = 0u64;

    // Timing histogram buckets (ms): <1, 1-5, 5-10, 10-20, 20-50, >=50
    let mut hist = [0u64; 6];

    while let Some(dir_prefix) = queue.pop_front() {
        let dir_t0 = Instant::now();
        let scan = scan_l1_dir(metadata, root, &dir_prefix, &ScanScope::Bucket)
            .await
            .expect("scan_l1_dir failed");
        let dir_elapsed_ms = dir_t0.elapsed().as_secs_f64() * 1000.0;

        dirs += 1;
        new_files += scan.new_count;
        unchanged += scan.unchanged;

        // Commit this directory's write op immediately (single-writer simulation).
        if !scan.write_op.inserts.is_empty() || !scan.write_op.stale_names.is_empty() {
            let ops = vec![scan.write_op];
            metadata
                .l1_batch_write(&ops)
                .await
                .expect("l1_batch_write failed");
        }

        let bucket = match dir_elapsed_ms as u64 {
            0 => 0,
            1..=4 => 1,
            5..=9 => 2,
            10..=19 => 3,
            20..=49 => 4,
            _ => 5,
        };
        hist[bucket] += 1;

        for child in scan.child_dirs {
            queue.push_back(child);
        }

        if dirs % 100 == 0 {
            eprintln!(
                "  [{label}] {dirs} dirs, {new_files} new, {unchanged} unchanged, {:.1}s elapsed",
                t0.elapsed().as_secs_f64()
            );
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();

    eprintln!("\n=== {label} pass ===");
    eprintln!("  Dirs scanned : {dirs}");
    eprintln!("  New files    : {new_files}");
    eprintln!("  Unchanged    : {unchanged}");
    eprintln!("  Elapsed      : {elapsed:.2}s  ({:.1} dirs/s)", dirs as f64 / elapsed);
    eprintln!("  Dir latency histogram (ms):");
    eprintln!("    <1ms   : {}", hist[0]);
    eprintln!("    1-5ms  : {}", hist[1]);
    eprintln!("    5-10ms : {}", hist[2]);
    eprintln!("    10-20ms: {}", hist[3]);
    eprintln!("    20-50ms: {}", hist[4]);
    eprintln!("    >=50ms : {}", hist[5]);

    (dirs, new_files, unchanged, elapsed)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".parse().unwrap()),
        )
        .init();

    let dir = std::env::args()
        .nth(1)
        .expect("Usage: bench_l1_bfs <directory>");
    let root = PathBuf::from(&dir);
    assert!(root.is_dir(), "{dir} is not a directory");

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp DB");
    let metadata = MetadataStore::new(tmp.path()).await.unwrap();

    eprintln!("=== bench_l1_bfs: {dir} ===\n");

    // Pass 1: Cold — empty catalog, everything is new
    let (dirs, _, _, cold_secs) = run_bfs(&metadata, &root, "COLD").await;

    // Pass 2: Warm — full catalog, everything should be unchanged
    let (_, _, warm_unchanged, warm_secs) = run_bfs(&metadata, &root, "WARM").await;

    eprintln!("\n=== Summary ===");
    eprintln!("  Dirs          : {dirs}");
    eprintln!("  Cold scan     : {cold_secs:.2}s");
    eprintln!("  Warm scan     : {warm_secs:.2}s");
    eprintln!("  Warm unchanged: {warm_unchanged}");
    eprintln!(
        "  Cold dir/s    : {:.1}",
        dirs as f64 / cold_secs
    );
    eprintln!(
        "  Warm dir/s    : {:.1}",
        dirs as f64 / warm_secs
    );
}
