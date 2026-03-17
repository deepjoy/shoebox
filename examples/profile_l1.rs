//! Profile L1 scan on a real directory with flamegraph output.
//!
//! Usage:
//!   cargo run --release --example profile_l1 -- /path/to/large/directory
//!
//! Produces:
//!   flamegraph_l1_cold.svg  — first scan (empty catalog, all inserts)
//!   flamegraph_l1_warm.svg  — second scan (full catalog, all skips)
//!   profile_l1.txt          — collapsed stacks for both passes (for analysis)

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use pprof::flamegraph::Options;
use shoebox::metadata::MetadataStore;
use shoebox::scanner::levels::scan_l1;
use shoebox::scanner::scope::ScanScope;

fn write_outputs(
    guard: pprof::ProfilerGuard,
    svg_path: &str,
    title: String,
    collapsed_out: &mut impl Write,
    label: &str,
) {
    let report = guard.report().build().expect("failed to build profile report");

    // Write SVG flamegraph
    let mut file = File::create(svg_path).expect("failed to create SVG file");
    let mut opts = Options::default();
    opts.title = title;
    report
        .flamegraph_with_options(&mut file, &mut opts)
        .expect("failed to write flamegraph");
    eprintln!("  -> {svg_path}");

    // Append collapsed stacks to text file
    writeln!(collapsed_out, "=== {label} ===").unwrap();
    for (frames, count) in report.data.iter() {
        let stack: Vec<String> = frames.frames.iter().rev().map(|f| {
            f.iter().map(|s| s.name()).collect::<Vec<_>>().join("|")
        }).collect();
        writeln!(collapsed_out, "{} {}", stack.join(";"), count).unwrap();
    }
    writeln!(collapsed_out).unwrap();
}

fn start_profiler() -> pprof::ProfilerGuard<'static> {
    pprof::ProfilerGuardBuilder::default()
        .frequency(997) // prime to avoid aliasing with timer interrupts
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("failed to start profiler")
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shoebox::scanner=info".parse().unwrap()),
        )
        .init();

    let dir = std::env::args()
        .nth(1)
        .expect("Usage: profile_l1 <directory>");
    let root = PathBuf::from(&dir);
    assert!(root.is_dir(), "{dir} is not a directory");

    // Temporary metadata DB (discarded after run)
    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    let metadata = MetadataStore::new(tmp.path()).await.unwrap();
    let mut collapsed = File::create("profile_l1.txt").expect("failed to create text file");

    // === Pass 1: Cold scan (empty catalog → all inserts) ===
    eprintln!("=== Pass 1 (cold): {dir} ===");
    let guard = start_profiler();
    let t0 = std::time::Instant::now();
    let report = scan_l1(&metadata, &root, &ScanScope::Bucket).await.unwrap();
    let elapsed = t0.elapsed();
    eprintln!("  {elapsed:.2?} — {report:#?}");
    write_outputs(guard, "flamegraph_l1_cold.svg", format!("L1 cold scan: {dir} ({elapsed:.2?})"), &mut collapsed, &format!("COLD ({elapsed:.2?}, {dir})"));

    // === Pass 2: Warm scan (full catalog → all skips) ===
    eprintln!("=== Pass 2 (warm): {dir} ===");
    let guard = start_profiler();
    let t0 = std::time::Instant::now();
    let report = scan_l1(&metadata, &root, &ScanScope::Bucket).await.unwrap();
    let elapsed = t0.elapsed();
    eprintln!("  {elapsed:.2?} — {report:#?}");
    write_outputs(guard, "flamegraph_l1_warm.svg", format!("L1 warm scan: {dir} ({elapsed:.2?})"), &mut collapsed, &format!("WARM ({elapsed:.2?}, {dir})"));

    eprintln!("=== Flamegraphs + profile_l1.txt written ===");
}
