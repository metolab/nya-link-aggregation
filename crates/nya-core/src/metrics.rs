use std::sync::atomic::{AtomicU64, Ordering};

use super::path::PathState;

#[derive(Default)]
pub struct Counters {
    pub path_added: AtomicU64,
    pub path_down: AtomicU64,
    pub migrates: AtomicU64,
    pub failbacks: AtomicU64,
    pub failbacks_upgrade: AtomicU64,
    pub failbacks_class_empty: AtomicU64,
    pub hol_rebalances: AtomicU64,
    pub stream_resets: AtomicU64,
    pub bytes_data_tx: AtomicU64,
    pub bytes_data_rx: AtomicU64,
    pub frame_send_drop: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct PathSnap {
    pub name: String,
    pub rtt_us: u64,
    pub stable_rtt_us: u64,
    pub class_rtt_us: u64,
    pub inflight: u64,
    pub sticky: u64,
    pub alive: bool,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub path_added: u64,
    pub path_down: u64,
    pub migrates: u64,
    pub failbacks: u64,
    pub failbacks_upgrade: u64,
    pub failbacks_class_empty: u64,
    pub hol_rebalances: u64,
    pub stream_resets: u64,
    pub bytes_data_tx: u64,
    pub bytes_data_rx: u64,
    pub frame_send_drop: u64,
    pub paths: Vec<PathSnap>,
}

impl Counters {
    pub fn snap_with_paths(&self, paths: &[std::sync::Arc<PathState>]) -> Snapshot {
        Snapshot {
            path_added: self.path_added.load(Ordering::Relaxed),
            path_down: self.path_down.load(Ordering::Relaxed),
            migrates: self.migrates.load(Ordering::Relaxed),
            failbacks: self.failbacks.load(Ordering::Relaxed),
            failbacks_upgrade: self.failbacks_upgrade.load(Ordering::Relaxed),
            failbacks_class_empty: self.failbacks_class_empty.load(Ordering::Relaxed),
            hol_rebalances: self.hol_rebalances.load(Ordering::Relaxed),
            stream_resets: self.stream_resets.load(Ordering::Relaxed),
            bytes_data_tx: self.bytes_data_tx.load(Ordering::Relaxed),
            bytes_data_rx: self.bytes_data_rx.load(Ordering::Relaxed),
            frame_send_drop: self.frame_send_drop.load(Ordering::Relaxed),
            paths: paths
                .iter()
                .map(|p| PathSnap {
                    name: p.name.clone(),
                    rtt_us: p.rtt_us(),
                    stable_rtt_us: p.stable_rtt().as_micros() as u64,
                    class_rtt_us: p.class_rtt().as_micros() as u64,
                    inflight: p.inflight_bytes(),
                    sticky: p.sticky_count(),
                    alive: p.is_alive(),
                })
                .collect(),
        }
    }
}
