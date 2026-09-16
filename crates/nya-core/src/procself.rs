//! Process self-metrics from `/proc/self` (Linux only; `None` elsewhere).
//!
//! The host view that found the v0.1.7 idle spin and the hytron fd leak
//! came from `top`; these make the same three numbers scrapeable so a
//! regression shows up in Signoz, not in a shell.

use std::sync::OnceLock;

static FD_BASELINE: OnceLock<u64> = OnceLock::new();

/// CPU time consumed by this process (user + system), milliseconds.
/// `/proc/self/stat` reports in `USER_HZ` ticks, which Linux fixes at 100
/// for the proc interface regardless of the kernel's `HZ`.
pub fn cpu_ms() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Field 2 (comm) may contain spaces; everything after the closing ')'
    // is space-separated, starting at field 3 (state).
    let rest = &s[s.rfind(')')? + 1..];
    let mut it = rest.split_ascii_whitespace();
    // Fields 3..=13 are eleven values before utime (14) and stime (15).
    let utime: u64 = it.nth(11)?.parse().ok()?;
    let stime: u64 = it.next()?.parse().ok()?;
    Some((utime + stime) * 10)
}

/// Open file descriptors of this process.
pub fn open_fds() -> Option<u64> {
    let n = std::fs::read_dir("/proc/self/fd").ok()?.count() as u64;
    // `read_dir` holds one fd itself while iterating.
    Some(n.saturating_sub(1))
}

/// Resident set size in bytes (`VmRSS` from `/proc/self/status`; page-size
/// independent, unlike `statm`).
pub fn rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_ascii_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Record the process's static fd count once, before any session, listener
/// or dial exists. `nya_process_open_fds - baseline` is then the fds owed
/// to live work (paths, hops, listeners). Idempotent; first call wins.
pub fn mark_fd_baseline() {
    let _ = FD_BASELINE.set(open_fds().unwrap_or(0));
}

pub fn fd_baseline() -> u64 {
    FD_BASELINE.get().copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn reads_proc_self() {
        assert!(cpu_ms().is_some());
        let fds = open_fds().expect("fd count");
        // stdin/stdout/stderr at minimum.
        assert!(fds >= 3, "fds={fds}");
        assert!(rss_bytes().unwrap_or(0) > 0);
        mark_fd_baseline();
        assert!(fd_baseline() >= 3);
        // Other test threads open and close fds concurrently, so only a
        // one-sided check is stable: holding 16 extra files must show.
        let before = open_fds().unwrap();
        let held: Vec<_> = (0..16)
            .map(|_| std::fs::File::open("/proc/self/stat").unwrap())
            .collect();
        assert!(open_fds().unwrap() >= before + 8, "{before}");
        drop(held);
    }

    #[test]
    fn stat_parser_handles_spaces_in_comm() {
        // Not a live read: exercise the field arithmetic on a fixture.
        let fixture =
            "1234 (tokio rt (x)) S 1 1 1 0 -1 4194560 100 0 0 0 250 75 0 0 20 0 2 0 100 1 1 1";
        let rest = &fixture[fixture.rfind(')').unwrap() + 1..];
        let mut it = rest.split_ascii_whitespace();
        let utime: u64 = it.nth(11).unwrap().parse().unwrap();
        let stime: u64 = it.next().unwrap().parse().unwrap();
        assert_eq!((utime, stime), (250, 75));
    }
}
