use std::time::Instant;

use sysinfo::{Disks, System};

use crate::resource::{ResourceSampler, ResourceSnapshot};

/// Cross-platform resource sampler using the `sysinfo` crate.
///
/// Works on Linux, macOS, and Windows. Tracks CPU utilization and
/// aggregate disk IO throughput across all mounted disks.
pub struct SysinfoSampler {
    sys: System,
    disks: Disks,
    prev_read_bytes: u64,
    prev_write_bytes: u64,
    prev_sample: Option<Instant>,
}

impl SysinfoSampler {
    pub fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_usage();

        let disks = Disks::new_with_refreshed_list();

        // Take initial disk totals so first delta is meaningful.
        let (read, write) = disk_totals(&disks);

        Self {
            sys,
            disks,
            prev_read_bytes: read,
            prev_write_bytes: write,
            prev_sample: Some(Instant::now()),
        }
    }
}

impl Default for SysinfoSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceSampler for SysinfoSampler {
    fn sample(&mut self) -> ResourceSnapshot {
        // CPU: sysinfo needs two refresh calls to compute usage delta.
        self.sys.refresh_cpu_usage();
        let cpu_usage = self.sys.global_cpu_usage() as f64 / 100.0;

        // Disk IO: compute bytes/sec since last sample.
        self.disks.refresh(true);
        let (read_bytes, write_bytes) = disk_totals(&self.disks);
        let now = Instant::now();

        let (read_bps, write_bps) = if let Some(prev_ts) = self.prev_sample {
            let elapsed = now.duration_since(prev_ts).as_secs_f64();
            if elapsed > 0.0 {
                let read_delta = read_bytes.saturating_sub(self.prev_read_bytes);
                let write_delta = write_bytes.saturating_sub(self.prev_write_bytes);
                (read_delta as f64 / elapsed, write_delta as f64 / elapsed)
            } else {
                (0.0, 0.0)
            }
        } else {
            (0.0, 0.0)
        };

        self.prev_read_bytes = read_bytes;
        self.prev_write_bytes = write_bytes;
        self.prev_sample = Some(now);

        ResourceSnapshot {
            cpu_usage,
            io_read_bytes_per_sec: read_bps,
            io_write_bytes_per_sec: write_bps,
        }
    }
}

/// Sum read/write bytes across all disks.
fn disk_totals(disks: &Disks) -> (u64, u64) {
    let mut total_read = 0u64;
    let mut total_write = 0u64;
    for disk in disks.list() {
        // sysinfo::Disk exposes usage(); total/available space but not IO counters
        // directly. We use the disk-level process IO as a proxy.
        // Note: sysinfo 0.33+ tracks disk IO via the Disks API on supported platforms.
        let usage = disk.usage();
        total_read += usage.read_bytes;
        total_write += usage.written_bytes;
    }
    (total_read, total_write)
}
