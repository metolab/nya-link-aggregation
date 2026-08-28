//! RTT-adaptive loss / path-down / failback timers.
//!
//! Timeouts are derived from the path's *stable* RTT so a 10ms link is
//! judged on a ~20ms clock and a 200ms link on a ~400ms clock.

use std::time::Duration;

use crate::cfg::SessionConfig;
use crate::tuning::{clamp, scale};

/// How long we wait for an ACK/Pong before calling the attempt lost.
pub fn loss_timeout(cfg: &SessionConfig, stable_rtt: Duration) -> Duration {
    cfg.tuning.loss_timeout(stable_rtt)
}

/// Silence after which a path is marked down (includes probe spacing).
pub fn down_timeout(cfg: &SessionConfig, stable_rtt: Duration, probe: Duration) -> Duration {
    cfg.tuning.down_timeout(stable_rtt, probe)
}

pub fn probe_interval(cfg: &SessionConfig, stable_rtt: Duration) -> Duration {
    clamp(stable_rtt, cfg.ping_interval_min, cfg.ping_interval_max)
}

pub fn is_backup(cfg: &SessionConfig, rtt: Duration, min_rtt: Duration) -> bool {
    rtt > scale(
        min_rtt,
        cfg.tuning.backup_rtt_mult,
        cfg.tuning.backup_rtt_add,
    )
}

/// Same-class failback threshold: `max(abs, better_rtt * frac)`.
///
/// `frac` must outrun typical WAN jitter (~0.2–0.3 of RTT at 60–200ms).
/// The 8ms abs floor still binds on 11–16ms lab peers.
pub fn failback_delta(cfg: &SessionConfig, better_rtt: Duration) -> Duration {
    cfg.tuning.failback_delta(better_rtt)
}

/// Do not hop again until at least one RTT has passed on the new path.
pub fn failback_cooldown(cfg: &SessionConfig, rtt: Duration) -> Duration {
    cfg.tuning.failback_cooldown_for(rtt)
}

/// Candidate must look stable for at least ~2 RTT before we fail back to it.
pub fn failback_stable(cfg: &SessionConfig, rtt: Duration) -> Duration {
    cfg.tuning.failback_stable_for(rtt)
}

pub fn should_failback(cfg: &SessionConfig, current_rtt: Duration, better_rtt: Duration) -> bool {
    if better_rtt >= current_rtt {
        return false;
    }
    cfg.tuning.class_jump(current_rtt, better_rtt)
        || current_rtt.saturating_sub(better_rtt) >= failback_delta(cfg, better_rtt)
}

/// RTT used for down/degrade clocks. Unknown paths use 2× ping_interval_max
/// so a 150–200ms first Pong is not judged on the 20ms placeholder.
pub fn assumed_rtt(
    cfg: &SessionConfig,
    rtt_known: bool,
    current: Duration,
    stable: Duration,
) -> Duration {
    if rtt_known {
        current.max(stable)
    } else {
        current.max(stable).max(cfg.ping_interval_max * 2)
    }
}

/// Phase-1 silence: 2× stable RTT. Unknown RTT is floored at `unknown_degrade_min`.
pub fn degrade_timeout(cfg: &SessionConfig, rtt_known: bool, stable: Duration) -> Duration {
    let rtt = if rtt_known {
        stable
    } else {
        stable.max(cfg.ping_interval_max * 2)
    };
    let t = loss_timeout(cfg, rtt);
    if rtt_known {
        t
    } else {
        t.max(cfg.tuning.unknown_degrade_min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loss_timeout_tracks_path_class() {
        let cfg = SessionConfig::default();
        assert_eq!(
            loss_timeout(&cfg, Duration::from_millis(10)),
            Duration::from_millis(20)
        );
        assert_eq!(
            loss_timeout(&cfg, Duration::from_millis(200)),
            Duration::from_millis(400)
        );
        // floor
        assert_eq!(
            loss_timeout(&cfg, Duration::from_millis(5)),
            Duration::from_millis(20)
        );
    }

    #[test]
    fn unknown_down_grace_covers_200ms_first_pong() {
        let cfg = SessionConfig::default();
        let assumed = cfg.ping_interval_max * 2;
        let probe = probe_interval(&cfg, assumed);
        let d = down_timeout(&cfg, assumed, probe);
        assert!(
            d >= Duration::from_millis(200),
            "unknown-path down timeout {d:?} must outlive a 200ms first Pong"
        );
    }

    #[test]
    fn down_timeout_covers_delay_spike_extra_on_fast_path() {
        let cfg = SessionConfig::default();
        let rtt = Duration::from_millis(12);
        let d = down_timeout(&cfg, rtt, probe_interval(&cfg, rtt));
        assert!(
            d > Duration::from_millis(274),
            "down {d:?} must outlive lossless extra=250ms silence"
        );
        assert!(d < Duration::from_millis(1000));
    }

    #[test]
    fn degrade_is_2x_on_known_12ms() {
        let cfg = SessionConfig::default();
        let d = degrade_timeout(&cfg, true, Duration::from_millis(12));
        assert!(
            d >= Duration::from_millis(24) && d <= Duration::from_millis(40),
            "degrade {d:?}"
        );
    }

    #[test]
    fn unknown_degrade_covers_200ms_first_pong() {
        let cfg = SessionConfig::default();
        let d = degrade_timeout(&cfg, false, Duration::from_millis(20));
        assert!(d >= Duration::from_millis(200), "unknown degrade {d:?}");
    }

    #[test]
    fn backup_is_twice_min_plus_slack() {
        let cfg = SessionConfig::default();
        let min = Duration::from_millis(10);
        assert!(!is_backup(&cfg, Duration::from_millis(12), min));
        assert!(is_backup(&cfg, Duration::from_millis(60), min));
        assert!(!is_backup(
            &cfg,
            Duration::from_millis(65),
            Duration::from_millis(60)
        ));
    }

    #[test]
    fn failback_when_better_is_clearly_faster() {
        let cfg = SessionConfig::default();
        assert!(should_failback(
            &cfg,
            Duration::from_millis(60),
            Duration::from_millis(10)
        ));
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(12),
            Duration::from_millis(10)
        ));
        // 8ms abs floor: 12–16ms peers stay same class.
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(15),
            Duration::from_millis(12)
        ));
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(16),
            Duration::from_millis(12)
        ));
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(14),
            Duration::from_millis(12)
        ));
        // 21 vs 9 is abs (class-jump needs 21.5ms).
        assert!(should_failback(
            &cfg,
            Duration::from_millis(21),
            Duration::from_millis(9)
        ));
        // Slow class leave: 28 vs 16 via abs, 30 vs 16 via abs.
        assert!(should_failback(
            &cfg,
            Duration::from_millis(28),
            Duration::from_millis(16)
        ));
        assert!(should_failback(
            &cfg,
            Duration::from_millis(30),
            Duration::from_millis(16)
        ));
        // 5ms is noise on a 60ms path (0.45*60=27ms), not a class change.
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(65),
            Duration::from_millis(60)
        ));
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(80),
            Duration::from_millis(60)
        ));
        assert!(should_failback(
            &cfg,
            Duration::from_millis(90),
            Duration::from_millis(60)
        ));
        // 40ms jitter on a 180ms path must not flap same-class peers.
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(210),
            Duration::from_millis(180)
        ));
        // delay_shift to ~1.7× still failbacks (306 vs 180).
        assert!(should_failback(
            &cfg,
            Duration::from_millis(306),
            Duration::from_millis(180)
        ));
        assert!(cfg
            .tuning
            .class_jump(Duration::from_millis(306), Duration::from_millis(180)));
        assert!(!cfg
            .tuning
            .class_jump(Duration::from_millis(270), Duration::from_millis(180)));
        assert_eq!(
            failback_delta(&cfg, Duration::from_millis(12)),
            Duration::from_millis(8)
        );
        assert_eq!(
            failback_delta(&cfg, Duration::from_millis(60)),
            Duration::from_millis(27)
        );
        // Native far spread stays same-class; do not widen frac to "fix" it.
        assert!(!should_failback(
            &cfg,
            Duration::from_millis(198),
            Duration::from_millis(162)
        ));
        // Mid 62 vs 96 is already a class split.
        assert!(should_failback(
            &cfg,
            Duration::from_millis(96),
            Duration::from_millis(62)
        ));
    }

    #[test]
    fn failback_timers_track_rtt() {
        let cfg = SessionConfig::default();
        assert_eq!(
            failback_cooldown(&cfg, Duration::from_millis(12)),
            cfg.tuning.failback_cooldown
        );
        assert_eq!(
            failback_cooldown(&cfg, Duration::from_millis(180)),
            Duration::from_millis(360)
        );
        assert_eq!(
            failback_stable(&cfg, Duration::from_millis(12)),
            cfg.tuning.failback_stable
        );
        assert_eq!(
            failback_stable(&cfg, Duration::from_millis(180)),
            Duration::from_millis(360)
        );
    }
}
