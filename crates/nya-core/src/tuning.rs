//! Health / failback / class / IO / reconnect formulas — not TOML.
//!
//! [`crate::cfg::SessionConfig`] is the operator surface: probe budget, path
//! cap, give-up timer. Runtime reads `cfg.tuning`. [`Tuning::STANDARD`] is
//! the default table, not a global.

use std::time::Duration;

/// Hidden overlay parameters. Clone a [`Tuning::STANDARD`] and mutate in tests.
#[derive(Clone, Debug, PartialEq)]
pub struct Tuning {
    // --- health extras (formula is `clamp(rtt * mult + add, floor, ceil)`) ---
    /// Maintain tick. Must be << loss timeout on fast paths.
    pub maintain_interval: Duration,
    pub loss_timeout_mult: f64,
    pub loss_timeout_floor: Duration,
    pub loss_timeout_add: Duration,
    pub loss_timeout_ceil: Duration,
    pub down_timeout_mult: f64,
    pub down_timeout_add: Duration,
    pub down_timeout_ceil: Duration,
    /// Floor on path-down silence (then +probe). Keeps 80–250ms delay spikes
    /// from tearing TCP on fast paths; 5×RTT still binds on slow paths.
    pub down_min_silence: Duration,
    /// First-Pong floor for degrade when RTT is still unknown.
    pub unknown_degrade_min: Duration,
    pub backup_rtt_mult: f64,
    pub backup_rtt_add: Duration,
    /// Class-jump failback: current ≥ better × mult + add.
    pub failback_class_mult: f64,
    pub failback_class_add: Duration,
    /// Same-class failback floor. Actual delta is
    /// `max(failback_abs, better_rtt * failback_abs_frac)`.
    pub failback_abs: Duration,
    pub failback_abs_frac: f64,
    pub failback_stable: Duration,
    pub failback_cooldown: Duration,
    /// Fast RTT must stay high this long before stable RTT (loss budget) rises.
    pub stable_up_hold: Duration,

    // --- path RTT ---
    /// Assumed RTT (µs) before the first ping completes.
    pub unknown_rtt_us: u64,
    /// Extra score penalty for unknown RTT in path pick.
    pub unknown_rtt_score_mult: u64,
    /// Raise stable RTT only if fast > stable × this and + add_us.
    pub stable_raise_mult: u64,
    pub stable_raise_add_us: u64,
    /// Class-RTT drop floor (µs). Actual need is
    /// `max(abs, class * class_drop_frac)` for `stable_up_hold`, then 7/8.
    /// An 8ms-only gate follows jitter low-tail at 60–200ms and collapses
    /// one peer into a singleton fastest class (Upgrade chatter).
    pub class_drop_abs_us: u64,
    /// Of *class*. 0.25 recovers a 2-step 7/8 raise (~1.35×) and ignores
    /// 0.20–0.22 RTT jitter. 0.45-of-fast left class stuck high so slow
    /// joined fastest_class.
    pub class_drop_frac: f64,
    pub pending_ping_max: usize,

    // --- IO / QoS ---
    /// Path writer, incoming, and inbound channel depth.
    pub chan: usize,
    /// STREAM_DATA ≤ this rides the urgent writer; bulk ACKs are not RTT.
    pub interactive_max: usize,
    pub ack_rtt_min: Duration,
    pub ack_rtt_max: Duration,

    // --- scheduler / stream ---
    pub inflight_bias: u64,
    pub min_rebalance_slack: u64,
    pub initial_window: u32,

    // --- wait / reconnect ---
    pub handshake_timeout: Duration,
    pub reconnect_backoff_min: Duration,
    pub reconnect_backoff_max: Duration,
    pub join_poll: Duration,
    pub ready_poll: Duration,
}

impl Tuning {
    pub const STANDARD: Self = Self {
        maintain_interval: Duration::from_millis(5),
        loss_timeout_mult: 2.0,
        loss_timeout_floor: Duration::from_millis(20),
        loss_timeout_add: Duration::ZERO,
        loss_timeout_ceil: Duration::from_millis(2000),
        down_timeout_mult: 5.0,
        down_timeout_add: Duration::ZERO,
        down_timeout_ceil: Duration::from_millis(5000),
        down_min_silence: Duration::from_millis(320),
        unknown_degrade_min: Duration::from_millis(300),
        backup_rtt_mult: 2.0,
        backup_rtt_add: Duration::from_millis(20),
        failback_class_mult: 1.5,
        failback_class_add: Duration::from_millis(8),
        failback_abs: Duration::from_millis(8),
        failback_abs_frac: 0.45,
        failback_stable: Duration::from_millis(250),
        failback_cooldown: Duration::from_millis(200),
        stable_up_hold: Duration::from_millis(1000),
        unknown_rtt_us: 20_000,
        unknown_rtt_score_mult: 2,
        stable_raise_mult: 2,
        stable_raise_add_us: 15_000,
        class_drop_abs_us: 8_000,
        class_drop_frac: 0.25,
        pending_ping_max: 32,
        chan: 64,
        interactive_max: 1500,
        ack_rtt_min: Duration::from_micros(100),
        ack_rtt_max: Duration::from_secs(2),
        inflight_bias: 64 * 1024,
        min_rebalance_slack: 16 * 1024,
        initial_window: 128 * 1024,
        handshake_timeout: Duration::from_secs(3),
        reconnect_backoff_min: Duration::from_millis(200),
        reconnect_backoff_max: Duration::from_secs(2),
        join_poll: Duration::from_millis(20),
        ready_poll: Duration::from_millis(50),
    };

    pub fn rebalance_slack(&self) -> u64 {
        (self.inflight_bias / 2).max(self.min_rebalance_slack)
    }

    pub fn loss_timeout(&self, rtt: Duration) -> Duration {
        clamp(
            scale(rtt, self.loss_timeout_mult, self.loss_timeout_add),
            self.loss_timeout_floor,
            self.loss_timeout_ceil,
        )
    }

    pub fn down_timeout(&self, rtt: Duration, probe: Duration) -> Duration {
        let scaled = scale(rtt, self.down_timeout_mult, self.down_timeout_add) + probe;
        let min_silence = self.down_min_silence + probe;
        clamp(scaled.max(min_silence), min_silence, self.down_timeout_ceil)
    }

    pub fn class_jump(&self, current: Duration, better: Duration) -> bool {
        current >= scale(better, self.failback_class_mult, self.failback_class_add)
    }

    /// Same-class failback threshold: `max(abs, better_rtt * frac)`.
    pub fn failback_delta(&self, better_rtt: Duration) -> Duration {
        self.failback_abs
            .max(scale(better_rtt, self.failback_abs_frac, Duration::ZERO))
    }

    pub fn should_failback(&self, current: Duration, better: Duration) -> bool {
        if better >= current {
            return false;
        }
        self.class_jump(current, better)
            || current.saturating_sub(better) >= self.failback_delta(better)
    }

    /// RTT-adaptive hold. Named `_for` so it does not collide with the
    /// `failback_stable` field (raw const; steer uses that as a missing-path fallback).
    pub fn failback_stable_for(&self, rtt: Duration) -> Duration {
        rtt_hold(self.failback_stable, rtt)
    }

    pub fn failback_cooldown_for(&self, rtt: Duration) -> Duration {
        rtt_hold(self.failback_cooldown, rtt)
    }

    /// Class drops toward `fast` only when `class − fast` exceeds a
    /// fraction of *class* (or the 8ms floor on lab-near paths).
    pub fn class_should_drop(&self, class_us: u64, fast_us: u64) -> bool {
        let need = self
            .class_drop_abs_us
            .max((class_us as f64 * self.class_drop_frac) as u64);
        class_us.saturating_sub(fast_us) >= need
    }
}

impl Default for Tuning {
    fn default() -> Self {
        Self::STANDARD
    }
}

fn rtt_hold(floor: Duration, rtt: Duration) -> Duration {
    floor.max(rtt * 2)
}

pub(crate) fn clamp(d: Duration, floor: Duration, ceil: Duration) -> Duration {
    if d < floor {
        floor
    } else if d > ceil {
        ceil
    } else {
        d
    }
}

pub(crate) fn scale(rtt: Duration, mult: f64, add: Duration) -> Duration {
    let us = (rtt.as_micros() as f64 * mult) as u64;
    Duration::from_micros(us) + add
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_is_the_production_table() {
        let t = Tuning::STANDARD;
        assert_eq!(t.maintain_interval, Duration::from_millis(5));
        assert_eq!(t.loss_timeout_mult, 2.0);
        assert_eq!(t.loss_timeout_floor, Duration::from_millis(20));
        assert_eq!(t.loss_timeout_add, Duration::ZERO);
        assert_eq!(t.loss_timeout_ceil, Duration::from_millis(2000));
        assert_eq!(t.down_timeout_mult, 5.0);
        assert_eq!(t.down_timeout_ceil, Duration::from_millis(5000));
        assert_eq!(t.down_min_silence, Duration::from_millis(320));
        assert_eq!(t.unknown_degrade_min, Duration::from_millis(300));
        assert_eq!(t.backup_rtt_mult, 2.0);
        assert_eq!(t.backup_rtt_add, Duration::from_millis(20));
        assert_eq!(t.failback_class_mult, 1.5);
        assert_eq!(t.failback_class_add, Duration::from_millis(8));
        assert_eq!(t.failback_abs, Duration::from_millis(8));
        assert_eq!(t.failback_abs_frac, 0.45);
        assert_eq!(t.failback_stable, Duration::from_millis(250));
        assert_eq!(t.failback_cooldown, Duration::from_millis(200));
        assert_eq!(t.stable_up_hold, Duration::from_millis(1000));
        assert_eq!(t.class_drop_abs_us, 8_000);
        assert_eq!(t.class_drop_frac, 0.25);
        assert_eq!(t.unknown_rtt_us, 20_000);
        assert_eq!(t.interactive_max, 1500);
        assert_eq!(t.inflight_bias, 64 * 1024);
        assert_eq!(t.rebalance_slack(), 32 * 1024);
        assert_eq!(t.chan, 64);
        assert_eq!(t.handshake_timeout, Duration::from_secs(3));
    }

    #[test]
    fn class_drop_ignores_jitter_recovers_class_jump() {
        let t = Tuning::STANDARD;
        assert!(!t.class_should_drop(180_000, 140_000), "40ms jitter");
        assert!(!t.class_should_drop(220_000, 180_000), "220 vs 180");
        assert!(t.class_should_drop(280_000, 180_000), "280 vs 180");
        assert!(t.class_should_drop(20_000, 10_000), "8ms floor on near");
        assert!(!t.class_should_drop(96_000, 80_000), "mid jitter 16ms");
    }
}
