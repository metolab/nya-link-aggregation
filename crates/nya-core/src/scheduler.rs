//! Path pick, backup selection, and same-link TCP rebalance.
//!
//! New streams stay on the fastest RTT class, then spread across named
//! links in that class by `rtt * (1 + inflight/bias + sticky)`. HOL
//! isolation is same-link rebalance (`should_rebalance_conn`) and
//! sibling-first `backup_path`, not ISP pinning.

use std::sync::Arc;
use std::time::Duration;

use crate::cfg::SessionConfig;
use crate::health;
use crate::path::PathState;

fn rtt_score_us(p: &PathState, cfg: &SessionConfig) -> u64 {
    if p.rtt_known() {
        p.rtt_us()
    } else {
        p.rtt_us().saturating_mul(cfg.tuning.unknown_rtt_score_mult)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PickPref {
    Any,
    Interactive,
}

fn load_term(p: &PathState, cfg: &SessionConfig, pref: PickPref) -> u64 {
    let inf = p.inflight_bytes();
    let sticky = p.sticky_count();
    let bias = cfg.tuning.inflight_bias.max(1);
    match pref {
        PickPref::Any => 1 + inf / bias + sticky,
        PickPref::Interactive => 1 + inf / (bias / 4).max(1) + sticky,
    }
}

/// Membership / Upgrade RTT. Frozen class that is stale-high versus live
/// fast EWMA (`should_failback(class, fast)`) yields to fast so a recovered
/// peer is in `fastest_class` and can pull stickies off slow. A delay_spike
/// (`fast >> class`) keeps class, so 1.7× does not hop.
pub(crate) fn effective_class_rtt(cfg: &SessionConfig, p: &PathState) -> Duration {
    let class = p.class_rtt();
    if p.class_known() {
        let fast = p.rtt();
        if health::should_failback(cfg, class, fast) {
            return fast;
        }
    }
    class
}

fn min_class_rtt(paths: &[&Arc<PathState>], cfg: &SessionConfig) -> Duration {
    // Unfrozen class (init window) tracks fast EWMA — do not let a
    // lucky-low reconnect become the singleton fastest class.
    paths
        .iter()
        .filter(|p| p.class_known())
        .map(|p| effective_class_rtt(cfg, p))
        .min()
        .or_else(|| {
            paths
                .iter()
                .filter(|p| p.rtt_known())
                .map(|p| effective_class_rtt(cfg, p))
                .min()
        })
        .unwrap_or_else(|| {
            paths
                .iter()
                .map(|p| effective_class_rtt(cfg, p))
                .min()
                .unwrap_or(cfg.ping_interval_max)
        })
}

pub fn pick_path(paths: &[Arc<PathState>], cfg: &SessionConfig) -> Option<u32> {
    pick_path_pref(paths, cfg, PickPref::Any)
}

pub fn pick_path_pref(
    paths: &[Arc<PathState>],
    cfg: &SessionConfig,
    pref: PickPref,
) -> Option<u32> {
    pick_from(&fastest_class_set(paths, cfg), cfg, pref)
}

pub(crate) fn fastest_class_set<'a>(
    paths: &'a [Arc<PathState>],
    cfg: &SessionConfig,
) -> Vec<&'a Arc<PathState>> {
    let alive: Vec<&Arc<PathState>> = paths.iter().filter(|p| p.is_alive()).collect();
    if alive.is_empty() {
        return Vec::new();
    }
    let min_rtt = min_class_rtt(&alive, cfg);

    let mut candidates: Vec<&Arc<PathState>> = alive
        .iter()
        .copied()
        .filter(|p| {
            p.is_schedulable()
                && p.rtt_known()
                && !health::is_backup(cfg, effective_class_rtt(cfg, p), min_rtt)
        })
        .collect();
    if candidates.is_empty() {
        candidates = alive
            .iter()
            .copied()
            .filter(|p| p.is_schedulable())
            .collect();
    }
    if candidates.is_empty() {
        candidates = alive.iter().copied().filter(|p| p.is_up()).collect();
    }
    if candidates.is_empty() {
        candidates = alive;
    }

    // Restrict to the fastest *stable* RTT class. A busy 9ms link must not
    // spill new streams onto a healthy 21ms link. Fast EWMA spikes must
    // not move a 180ms peer into the slow class.
    if let Some(best) = candidates
        .iter()
        .filter(|p| p.class_known())
        .map(|p| effective_class_rtt(cfg, p))
        .min()
        .or_else(|| {
            candidates
                .iter()
                .filter(|p| p.rtt_known())
                .map(|p| effective_class_rtt(cfg, p))
                .min()
        })
    {
        let fast: Vec<&Arc<PathState>> = candidates
            .iter()
            .copied()
            .filter(|p| {
                let rtt = if p.class_known() {
                    effective_class_rtt(cfg, p)
                } else {
                    p.rtt()
                };
                !health::should_failback(cfg, rtt, best)
            })
            .collect();
        if !fast.is_empty() {
            candidates = fast;
        }
    }
    candidates
}

fn path_score(p: &PathState, cfg: &SessionConfig, pref: PickPref) -> (u64, bool) {
    let load = load_term(p, cfg, pref);
    let class = p.class_rtt().as_micros() as u64;
    let fast = rtt_score_us(p, cfg);
    // Class RTT dominates so a 1.7× spike on every peer cannot lose to a
    // same-class-by-cliff slow path on fast EWMA. Fast is the tie-break
    // among equal class_rtt (spiked peer loses to an unshifted sibling).
    let score = class
        .saturating_mul(load)
        .saturating_mul(1024)
        .saturating_add(fast.saturating_mul(load));
    (score, p.rtt_known())
}

pub(crate) fn pick_from(
    candidates: &[&Arc<PathState>],
    cfg: &SessionConfig,
    pref: PickPref,
) -> Option<u32> {
    if candidates.is_empty() {
        return None;
    }
    let mut best_id = candidates[0].id;
    let mut best_score = u64::MAX;
    let mut best_known = candidates[0].rtt_known();
    for p in candidates {
        let (score, known) = path_score(p, cfg, pref);
        let better = score < best_score
            || (score == best_score && known && !best_known)
            || (score == best_score && known == best_known && p.id < best_id);
        if better {
            best_score = score;
            best_id = p.id;
            best_known = known;
        }
    }
    Some(best_id)
}

/// Like [`pick_from`], but exact `(score, known)` ties rotate by `stream_id`.
/// Failback / HOL / backup keep [`pick_from`] (min id) so equal peers do not chatter.
pub(crate) fn pick_from_spread(
    candidates: &[&Arc<PathState>],
    cfg: &SessionConfig,
    pref: PickPref,
    stream_id: u32,
) -> Option<u32> {
    let best_id = pick_from(candidates, cfg, pref)?;
    let best = candidates.iter().find(|p| p.id == best_id)?;
    let (best_score, best_known) = path_score(best, cfg, pref);
    let mut tied: Vec<u32> = candidates
        .iter()
        .filter(|p| path_score(p, cfg, pref) == (best_score, best_known))
        .map(|p| p.id)
        .collect();
    if tied.is_empty() {
        return Some(best_id);
    }
    tied.sort_unstable();
    let n = tied.len() as u32;
    let idx = stream_id.wrapping_sub(1) % n;
    Some(tied[idx as usize])
}

pub fn pick_path_pref_spread(
    paths: &[Arc<PathState>],
    cfg: &SessionConfig,
    pref: PickPref,
    stream_id: u32,
) -> Option<u32> {
    pick_from_spread(&fastest_class_set(paths, cfg), cfg, pref, stream_id)
}

/// Frozen candidate dump for debug logs. Same score as [`pick_from`].
///
/// Grammar per candidate (id ascending, space-separated):
/// `{name}{id={id} {state} rtt={rtt_us} class={class_us} load={load} score={score}{*}?}`
/// Optional ` backup={name},...` for alive paths filtered out of the class set.
pub fn format_candidates(
    paths: &[Arc<PathState>],
    cfg: &SessionConfig,
    pref: PickPref,
    chosen: Option<u32>,
) -> String {
    use crate::metrics::path_state_label;
    let class = fastest_class_set(paths, cfg);
    let class_ids: std::collections::HashSet<u32> = class.iter().map(|p| p.id).collect();
    let mut cands: Vec<&Arc<PathState>> = class;
    cands.sort_by_key(|p| p.id);
    let mut parts = Vec::with_capacity(cands.len());
    for p in &cands {
        let load = load_term(p, cfg, pref);
        let class_us = p.class_rtt().as_micros() as u64;
        let fast = rtt_score_us(p, cfg);
        let score = class_us
            .saturating_mul(load)
            .saturating_mul(1024)
            .saturating_add(fast.saturating_mul(load));
        let star = if Some(p.id) == chosen { "*" } else { "" };
        parts.push(format!(
            "{}{{id={} {} rtt={} class={} load={} score={}{}}}",
            p.name,
            p.id,
            path_state_label(p.state.load(std::sync::atomic::Ordering::Relaxed)),
            p.rtt_us(),
            class_us,
            load,
            score,
            star
        ));
    }
    let mut out = parts.join(" ");
    let mut backups: Vec<&str> = paths
        .iter()
        .filter(|p| p.is_alive() && !class_ids.contains(&p.id))
        .map(|p| p.name.as_str())
        .collect();
    if !backups.is_empty() {
        backups.sort();
        out.push_str(" backup=");
        out.push_str(&backups.join(","));
    }
    out
}

/// HOL bulk fallback after same-link sibling: fastest class, no interactive,
/// `class_rtt <= cur`. Never moves bulk onto a higher class_rtt.
pub(crate) fn hol_place_bulk_fallback(
    paths: &[Arc<PathState>],
    cur: &PathState,
    cfg: &SessionConfig,
    conn_has_interactive: impl Fn(u32) -> bool,
) -> Option<u32> {
    let cands: Vec<&Arc<PathState>> = fastest_class_set(paths, cfg)
        .into_iter()
        .filter(|p| {
            p.id != cur.id
                && !conn_has_interactive(p.id)
                && effective_class_rtt(cfg, p) <= effective_class_rtt(cfg, cur)
        })
        .collect();
    pick_from(&cands, cfg, PickPref::Any)
}

#[cfg(test)]
pub fn path_elevated(cfg: &SessionConfig, p: &PathState) -> bool {
    cfg.tuning.class_jump(p.rtt(), p.class_rtt())
}

#[cfg(test)]
pub fn should_escape_spike(cfg: &SessionConfig, cur: &PathState, best: &PathState) -> bool {
    path_elevated(cfg, cur)
        && !path_elevated(cfg, best)
        && health::should_failback(cfg, cur.rtt(), best.rtt())
}

/// Others that are not a slower class than `cur` (`class_rtt`).
pub fn in_class_or_better<'a>(
    paths: &'a [Arc<PathState>],
    cur: &PathState,
    cfg: &SessionConfig,
) -> Vec<&'a Arc<PathState>> {
    paths
        .iter()
        .filter(|p| {
            p.is_schedulable()
                && p.id != cur.id
                && !health::should_failback(
                    cfg,
                    effective_class_rtt(cfg, p),
                    effective_class_rtt(cfg, cur),
                )
        })
        .collect()
}

pub fn pick_in_class(
    paths: &[Arc<PathState>],
    cur: &PathState,
    cfg: &SessionConfig,
) -> Option<u32> {
    let cands = in_class_or_better(paths, cur, cfg);
    if cands.is_empty() {
        return None;
    }
    pick_from(&cands, cfg, PickPref::Any)
}

/// Same class filter; UP or DEGRADED (not DOWN).
pub fn in_class_or_better_alive<'a>(
    paths: &'a [Arc<PathState>],
    cur: &PathState,
    cfg: &SessionConfig,
) -> Vec<&'a Arc<PathState>> {
    paths
        .iter()
        .filter(|p| {
            p.is_alive()
                && p.id != cur.id
                && !health::should_failback(
                    cfg,
                    effective_class_rtt(cfg, p),
                    effective_class_rtt(cfg, cur),
                )
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailbackReason {
    Upgrade,
    ClassEmpty,
}

/// Where a sticky should fail back, if anywhere.
pub fn failback_target(
    paths: &[Arc<PathState>],
    cur: &PathState,
    cfg: &SessionConfig,
) -> Option<(u32, FailbackReason)> {
    if let Some(best_id) = pick_in_class(paths, cur, cfg) {
        let best = paths.iter().find(|p| p.id == best_id)?;
        if cur.is_schedulable() {
            // Unfrozen dest class_rtt() is fast EWMA — do not pile onto it.
            if !best.class_known() {
                return None;
            }
            let fastest = fastest_class_set(paths, cfg);
            let cur_in = fastest.iter().any(|p| p.id == cur.id);
            let best_in = fastest.iter().any(|p| p.id == best_id);
            let cur_e = effective_class_rtt(cfg, cur);
            let best_e = effective_class_rtt(cfg, best);
            if cur_in {
                // Same fastest class: class-jump only. 0.45 among peers is
                // pick_path's job (that was Upgrade chatter). Compare
                // effective class so a recovered peer (stale-high class,
                // live-fast) does not class-jump onto slow.
                if !cfg.tuning.class_jump(cur_e, best_e) {
                    return None;
                }
            } else {
                // Outside fastest (slow, or a 0.45-split peer). Leave only
                // when fastest has ≥2 members — a singleton fastest is the
                // last-peer-elevated dump-to-slow (then bounce) path.
                // Gate on the class *min*, not load-weighted `best`: 258 vs
                // a busy 168ms peer picks 182, and 258 vs 182 is not 0.45.
                if fastest.len() < 2 || !best_in {
                    return None;
                }
                let min_fast = fastest
                    .iter()
                    .map(|p| effective_class_rtt(cfg, p))
                    .min()
                    .unwrap_or(best_e);
                if !health::should_failback(cfg, cur_e, min_fast) {
                    return None;
                }
            }
            if best.stable_for() < health::failback_stable(cfg, best_e) {
                return None;
            }
            return Some((best_id, FailbackReason::Upgrade));
        }
        // cur unusable. Far 180 vs 255 is in-class-by-cliff; do not dump to slow.
        // Same-link sibling is always an OK ClassEmpty dest.
        if effective_class_rtt(cfg, best) <= effective_class_rtt(cfg, cur)
            || best.link() == cur.link()
        {
            return Some((best_id, FailbackReason::ClassEmpty));
        }
    }
    if !cur.is_schedulable() {
        let sibs: Vec<&Arc<PathState>> = paths
            .iter()
            .filter(|p| p.id != cur.id && p.is_schedulable() && p.link() == cur.link())
            .collect();
        if let Some(id) = pick_from(&sibs, cfg, PickPref::Any) {
            return Some((id, FailbackReason::ClassEmpty));
        }
    }
    // No schedulable in-class others.
    if cur.is_alive() {
        return None;
    }
    let alive: Vec<&Arc<PathState>> = in_class_or_better_alive(paths, cur, cfg)
        .into_iter()
        .filter(|p| {
            p.link() == cur.link() || effective_class_rtt(cfg, p) <= effective_class_rtt(cfg, cur)
        })
        .collect();
    if !alive.is_empty() {
        let id = pick_from(&alive, cfg, PickPref::Any)?;
        return Some((id, FailbackReason::ClassEmpty));
    }
    let best_id = pick_path(paths, cfg)?;
    if best_id == cur.id {
        return None;
    }
    Some((best_id, FailbackReason::ClassEmpty))
}

#[allow(dead_code)]
pub fn backup_path(paths: &[Arc<PathState>], avoid: u32) -> Option<u32> {
    pick_backup(paths, avoid, None, None)
}

/// Timed-out unacked copy: never `avoid`, prefer a different named link.
pub fn pick_retry_path(paths: &[Arc<PathState>], cfg: &SessionConfig, avoid: u32) -> Option<u32> {
    let avoid_link = paths
        .iter()
        .find(|p| p.id == avoid)
        .map(|p| p.link().to_string());
    let pick_with = |pred: fn(&PathState) -> bool| -> Option<u32> {
        let diverse: Vec<&Arc<PathState>> = paths
            .iter()
            .filter(|p| {
                p.id != avoid
                    && pred(p)
                    && avoid_link.as_deref().map(|l| p.link() != l).unwrap_or(true)
            })
            .collect();
        if !diverse.is_empty() {
            return pick_from(&diverse, cfg, PickPref::Any);
        }
        let other: Vec<&Arc<PathState>> =
            paths.iter().filter(|p| p.id != avoid && pred(p)).collect();
        pick_from(&other, cfg, PickPref::Any)
    };
    pick_with(|p| p.is_schedulable()).or_else(|| pick_with(|p| p.is_alive()))
}

/// Same-link TCP rebalance: move off a loaded connection onto its sibling
/// even when RTTs are equal (bulk vs ping HOL isolation).
pub fn should_rebalance_conn(cur: &PathState, alt: &PathState, cfg: &SessionConfig) -> bool {
    if cur.id == alt.id || !alt.is_schedulable() {
        return false;
    }
    if cur.link() != alt.link() {
        return false;
    }
    // Inflight only — counting sticky streams oscillates as each flow hops.
    let slack = cfg.tuning.rebalance_slack();
    cur.inflight_bytes() > alt.inflight_bytes() + slack
}

/// Like `backup_path`, but only a candidate that is a real improvement
/// over `current_rtt` (same-class or class-jump failback rule).
#[allow(dead_code)]
pub fn backup_path_better(
    paths: &[Arc<PathState>],
    avoid: u32,
    current_rtt: Duration,
    cfg: &SessionConfig,
) -> Option<u32> {
    pick_backup(paths, avoid, Some((current_rtt, cfg)), None)
}

/// In-class schedulable dest, sibling-first (`avoid_link` from full `paths`).
/// If none and `cur` is alive, None. If `cur` is not alive, any-alive.
#[allow(dead_code)]
pub fn backup_prefer_class(
    paths: &[Arc<PathState>],
    avoid: u32,
    cfg: &SessionConfig,
) -> Option<u32> {
    let Some(cur) = paths.iter().find(|p| p.id == avoid) else {
        return backup_path(paths, avoid);
    };
    let mut ok: Vec<u32> = in_class_or_better(paths, cur, cfg)
        .into_iter()
        .filter(|p| {
            p.link() == cur.link() || effective_class_rtt(cfg, p) <= effective_class_rtt(cfg, cur)
        })
        .map(|p| p.id)
        .collect();
    // Same-link TCP is always eligible. Unknown-RTT (20ms fallback) vs a
    // sampled 100µs sibling looks like a class jump otherwise.
    for p in paths {
        if p.id != avoid && p.is_schedulable() && p.link() == cur.link() && !ok.contains(&p.id) {
            ok.push(p.id);
        }
    }
    if !ok.is_empty() {
        return pick_backup(paths, avoid, None, Some(&ok));
    }
    if cur.is_alive() {
        return None;
    }
    backup_path(paths, avoid)
}

fn pick_backup(
    paths: &[Arc<PathState>],
    avoid: u32,
    better: Option<(Duration, &SessionConfig)>,
    ok: Option<&[u32]>,
) -> Option<u32> {
    let avoid_link = paths
        .iter()
        .find(|p| p.id == avoid)
        .map(|p| p.link().to_string());
    let mut best: Option<(u32, u64, u8)> = None;
    for p in paths {
        if !p.is_alive() || p.id == avoid {
            continue;
        }
        if let Some(ids) = ok {
            if !ids.contains(&p.id) {
                continue;
            }
        }
        if let Some((cur, cfg)) = better {
            if !p.is_schedulable() {
                continue;
            }
            if p.rtt_known() {
                if !health::should_failback(cfg, cur, p.rtt()) {
                    continue;
                }
            } else {
                let min_known = paths
                    .iter()
                    .filter(|q| q.id != avoid && q.rtt_known())
                    .map(|q| q.rtt())
                    .min()
                    .unwrap_or(cfg.ping_interval_min);
                if !health::is_backup(cfg, cur, min_known) {
                    continue;
                }
            }
        }
        // Prefer a free/up *sibling TCP on the same link*, then any up path.
        let same_link = avoid_link
            .as_deref()
            .map(|l| p.link() == l)
            .unwrap_or(false);
        let class: u8 = if p.is_schedulable() && same_link {
            0
        } else if p.is_schedulable() {
            1
        } else if p.is_up() {
            2
        } else {
            3
        };
        let score = p.rtt().as_micros() as u64;
        match best {
            None => best = Some((p.id, score, class)),
            Some((_, _, c)) if class < c => best = Some((p.id, score, class)),
            Some((_, s, c)) if class == c && score < s => best = Some((p.id, score, class)),
            _ => {}
        }
    }
    best.map(|(id, _, _)| id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::SessionConfig;
    use crate::path::PathState;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    fn mk(id: u32, rtt_ms: u64) -> Arc<PathState> {
        mk_named(id, format!("p{id}"), rtt_ms)
    }

    fn mk_named(id: u32, name: String, rtt_ms: u64) -> Arc<PathState> {
        let (tx, _rx) = mpsc::channel(1);
        let p = PathState::new(id, name, tx);
        p.rtt_ewma_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p
    }

    fn mk_class(id: u32, name: &str, fast_ms: u64, class_ms: u64) -> Arc<PathState> {
        let p = mk_named(id, name.into(), fast_ms);
        p.rtt_stable_us.store(class_ms * 1000, Ordering::Relaxed);
        p.rtt_class_us.store(class_ms * 1000, Ordering::Relaxed);
        *p.up_since.lock().unwrap() = Instant::now() - Duration::from_secs(10);
        p
    }

    #[test]
    fn retry_prefers_other_named_link() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "akcdn#0".into(), 7);
        let a1 = mk_named(2, "akcdn#1".into(), 7);
        let b0 = mk_named(3, "soy#0".into(), 7);
        let picked = pick_retry_path(&[a0, a1, b0], &cfg, 1).unwrap();
        assert_eq!(picked, 3, "retry must leave the failed ISP");
    }

    #[test]
    fn retry_falls_back_to_sibling_when_only_one_link() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "akcdn#0".into(), 7);
        let a1 = mk_named(2, "akcdn#1".into(), 7);
        let picked = pick_retry_path(&[a0, a1], &cfg, 1).unwrap();
        assert_eq!(picked, 2);
    }

    #[test]
    fn retry_skips_congested_peer_when_a_free_link_exists() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "akcdn#0".into(), 7);
        let b0 = mk_named(2, "soy#0".into(), 7);
        let c0 = mk_named(3, "nsix#0".into(), 7);
        b0.set_congested(true);
        let picked = pick_retry_path(&[a0, b0, c0], &cfg, 1).unwrap();
        assert_eq!(picked, 3);
    }

    #[test]
    fn retry_none_when_only_avoid_is_alive() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "akcdn#0".into(), 7);
        assert!(pick_retry_path(&[a0], &cfg, 1).is_none());
    }

    #[test]
    fn spreads_across_equal_rtt_connections() {
        let cfg = SessionConfig::default();
        let a = mk(1, 10);
        let b = mk(2, 10);
        a.sticky_streams.store(3, Ordering::Relaxed);
        let picked = pick_path(&[a, b.clone()], &cfg).unwrap();
        assert_eq!(picked, 2, "empty sibling should win");
    }

    #[test]
    fn zero_load_sequential_spreads() {
        let cfg = SessionConfig::default();
        let paths = vec![
            mk_named(1, "a#0".into(), 7),
            mk_named(2, "a#1".into(), 7),
            mk_named(3, "b#0".into(), 7),
            mk_named(4, "b#1".into(), 7),
        ];
        let mut ids = Vec::new();
        for sid in 1..=8 {
            ids.push(pick_path_pref_spread(&paths, &cfg, PickPref::Any, sid).unwrap());
        }
        let uniq: std::collections::BTreeSet<_> = ids.iter().copied().collect();
        assert!(uniq.len() >= 2, "exact-score ties must spread, got {ids:?}");
        assert_eq!(ids[0], 1, "stream 1 maps to lowest path_id");
        assert_eq!(ids[1], 2);
        assert_eq!(ids[4], 1, "repeats after n");
    }

    #[test]
    fn load_beats_spread() {
        let cfg = SessionConfig::default();
        let a = mk_named(1, "a#0".into(), 7);
        let b = mk_named(2, "a#1".into(), 7);
        a.sticky_streams.store(3, Ordering::Relaxed);
        let paths = vec![a, b];
        for sid in 1..=6 {
            let picked = pick_path_pref_spread(&paths, &cfg, PickPref::Any, sid).unwrap();
            assert_eq!(
                picked, 2,
                "loaded sibling must lose every stream_id, sid={sid}"
            );
        }
    }

    #[test]
    fn in_class_6_vs_7_does_not_spread() {
        let cfg = SessionConfig::default();
        let a = mk_named(1, "a#0".into(), 6);
        let b = mk_named(2, "a#1".into(), 7);
        let paths = vec![a, b];
        for sid in 1..=4 {
            let picked = pick_path_pref_spread(&paths, &cfg, PickPref::Any, sid).unwrap();
            assert_eq!(picked, 1, "6ms vs 7ms is not an exact score tie, sid={sid}");
        }
    }

    #[test]
    fn format_candidates_score_monotonic_star_on_winner() {
        let cfg = SessionConfig::default();
        let a = mk_named(1, "a#0".into(), 10);
        let b = mk_named(2, "a#1".into(), 10);
        a.sticky_streams.store(3, Ordering::Relaxed);
        let paths = vec![a, b];
        let picked = pick_path(&paths, &cfg).unwrap();
        let s = format_candidates(&paths, &cfg, PickPref::Any, Some(picked));
        assert!(s.contains("a#0{id=1 up rtt="), "{s}");
        assert!(s.contains("a#1{id=2 up rtt="), "{s}");
        assert!(s.contains(&format!("id={picked}")), "{s}");
        assert!(s.contains("score="), "{s}");
        // Winner marked with * inside the braces.
        let star_at = s.find('*').expect("star");
        let id2 = s.find("id=2").expect("id=2");
        assert!(star_at > id2, "lower-load id=2 should be starred, got {s}");
        let score = |name: &str| -> u64 {
            let start = s.find(name).unwrap();
            let frag = &s[start..];
            let k = frag.find("score=").unwrap();
            let rest = &frag[k + 6..];
            let end = rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len());
            rest[..end].parse().unwrap()
        };
        assert!(
            score("a#0") > score("a#1"),
            "higher sticky must score worse: {s}"
        );
    }

    #[test]
    fn skips_congested_when_sibling_is_free() {
        let cfg = SessionConfig::default();
        let a = mk(1, 10);
        let b = mk(2, 10);
        a.set_congested(true);
        let picked = pick_path(&[a, b], &cfg).unwrap();
        assert_eq!(picked, 2);
    }

    #[test]
    fn backup_prefers_same_class_sibling() {
        let fast = mk(1, 10);
        let sib = mk(2, 11);
        let slow = mk(3, 60);
        let picked = backup_path(&[fast, sib, slow], 1).unwrap();
        assert_eq!(picked, 2);
    }

    #[test]
    fn backup_skips_congested_sibling() {
        let fast = mk(1, 10);
        let sib = mk(2, 11);
        sib.set_congested(true);
        let slow = mk(3, 60);
        let picked = backup_path(&[fast, sib, slow], 1).unwrap();
        assert_eq!(picked, 3);
    }

    #[test]
    fn ignores_backup_links_while_fast_exists() {
        let cfg = SessionConfig::default();
        let a = mk(1, 10);
        let b = mk(2, 60);
        a.sticky_streams.store(8, Ordering::Relaxed);
        let picked = pick_path(&[a, b], &cfg).unwrap();
        assert_eq!(picked, 1);
    }

    #[test]
    fn rebalance_same_link_when_inflight_skewed() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "a#0".into(), 9);
        let a1 = mk_named(2, "a#1".into(), 9);
        a0.inflight.store(100 * 1024, Ordering::Relaxed);
        a1.inflight.store(0, Ordering::Relaxed);
        assert!(should_rebalance_conn(&a0, &a1, &cfg));
        assert!(!should_rebalance_conn(&a1, &a0, &cfg));
        a0.inflight.store(20 * 1024, Ordering::Relaxed);
        assert!(
            !should_rebalance_conn(&a0, &a1, &cfg),
            "small skew must not flap"
        );
        let b = mk_named(3, "b#0".into(), 9);
        assert!(!should_rebalance_conn(&a0, &b, &cfg));
    }

    #[test]
    fn stays_on_fast_link_despite_bulk() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "a#0".into(), 9);
        let a1 = mk_named(2, "a#1".into(), 10);
        let b0 = mk_named(3, "b#0".into(), 21);
        a0.sticky_streams.store(8, Ordering::Relaxed);
        a0.inflight.store(200 * 1024, Ordering::Relaxed);
        let picked = pick_path(&[a0, a1.clone(), b0], &cfg).unwrap();
        assert_eq!(
            picked, 2,
            "empty conn on the fast link, not the slower link"
        );
    }

    #[test]
    fn does_not_spill_to_slower_link_when_fast_is_busy() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "a#0".into(), 9);
        let b0 = mk_named(2, "b#0".into(), 21);
        a0.sticky_streams.store(8, Ordering::Relaxed);
        a0.inflight.store(200 * 1024, Ordering::Relaxed);
        let picked = pick_path(&[a0, b0], &cfg).unwrap();
        assert_eq!(picked, 1, "busy 9ms link still beats empty 21ms link");
    }

    #[test]
    fn better_backup_skips_equal_or_worse() {
        let cfg = SessionConfig::default();
        let a = mk(1, 22);
        let sib = mk(2, 22);
        let slow = mk(3, 80);
        assert!(backup_path_better(&[a.clone(), sib, slow.clone()], 1, a.rtt(), &cfg).is_none());
        let fast = mk(4, 12);
        let picked = backup_path_better(&[a.clone(), fast, slow], 1, a.rtt(), &cfg).unwrap();
        assert_eq!(picked, 4);
    }

    #[test]
    fn mixes_same_class_named_links() {
        let cfg = SessionConfig::default();
        let paths = vec![
            mk_named(1, "a#0".into(), 12),
            mk_named(2, "a#1".into(), 12),
            mk_named(3, "b#0".into(), 14),
            mk_named(4, "b#1".into(), 14),
            mk_named(5, "c#0".into(), 16),
            mk_named(6, "c#1".into(), 16),
        ];
        let mut links = Vec::new();
        for _ in 0..6 {
            let id = pick_path(&paths, &cfg).unwrap();
            let p = paths.iter().find(|p| p.id == id).unwrap();
            p.add_sticky();
            links.push(crate::path::link_key(&p.name).to_string());
        }
        let uniq: std::collections::BTreeSet<_> = links.iter().cloned().collect();
        assert!(
            uniq.len() >= 2,
            "six picks must use more than one named link, got {links:?}"
        );
        assert_ne!(links[2], "a", "3rd pick should spill off a, got {links:?}");
    }

    #[test]
    fn busy_same_class_spills_to_peer() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "a#0".into(), 12);
        let a1 = mk_named(2, "a#1".into(), 12);
        let b0 = mk_named(3, "b#0".into(), 14);
        a0.sticky_streams.store(4, Ordering::Relaxed);
        a1.sticky_streams.store(4, Ordering::Relaxed);
        a0.inflight.store(200 * 1024, Ordering::Relaxed);
        a1.inflight.store(200 * 1024, Ordering::Relaxed);
        let picked = pick_path(&[a0, a1, b0], &cfg).unwrap();
        assert_eq!(picked, 3, "busy a must spill to empty in-class b");
    }

    #[test]
    fn spiked_peer_stays_in_class_and_loses_score() {
        let cfg = SessionConfig::default();
        let a = mk_class(1, "a#0", 306, 180);
        let b = mk_class(2, "b#0", 180, 180);
        let slow = mk_class(3, "s#0", 255, 255);
        let picked = pick_path(&[a.clone(), b.clone(), slow.clone()], &cfg).unwrap();
        assert_eq!(picked, 2, "fast spike must not dump pick onto slow");
        assert!(
            !health::should_failback(&cfg, a.class_rtt(), b.class_rtt()),
            "stable class still same"
        );
        assert_eq!(
            pick_path(&[a.clone(), slow.clone()], &cfg).unwrap(),
            1,
            "alone, spiked peer still beats slow on class_rtt"
        );
    }

    #[test]
    fn all_peer_1_7x_pick_stays_peer() {
        let cfg = SessionConfig::default();
        let peers: Vec<_> = (0..5)
            .map(|i| mk_class(i + 1, &format!("p{i}"), 306, 180))
            .collect();
        let slow = mk_class(9, "s", 255, 255);
        let mut paths = peers.clone();
        paths.push(slow);
        let picked = pick_path(&paths, &cfg).unwrap();
        assert!(
            picked <= 5,
            "all-peer 1.7× still picks a peer, got {picked}"
        );
    }

    #[test]
    fn far_class_cliff_min_182_includes_slow_but_scores_peer() {
        let cfg = SessionConfig::default();
        let c = mk_class(1, "c#0", 182, 182);
        let s1 = mk_class(2, "s1#0", 255, 255);
        let picked = pick_path(&[c, s1], &cfg).unwrap();
        assert_eq!(picked, 1);
    }

    #[test]
    fn spike_escape_1_7x_not_jitter() {
        let cfg = SessionConfig::default();
        let shifted = mk_class(1, "a#0", 306, 180);
        let jittered = mk_class(2, "a#1", 270, 180);
        let healthy = mk_class(3, "b#0", 180, 180);
        let slow = mk_class(4, "s#0", 255, 255);
        assert!(should_escape_spike(&cfg, &shifted, &healthy));
        assert!(!should_escape_spike(&cfg, &jittered, &healthy));
        // 1.7× delay_shift and delay_spike look identical on fast EWMA.
        // Hopping on class_jump(fast, stable) exploded failbacks (34–600/min).
        assert!(
            failback_target(&[shifted.clone(), healthy.clone(), slow], &shifted, &cfg).is_none(),
            "1.7× must not failback-hop (indistinguishable from delay_spike)"
        );
        assert!(
            failback_target(&[jittered.clone(), healthy], &jittered, &cfg).is_none(),
            "90ms jitter must not hop"
        );
    }

    #[test]
    fn last_in_class_does_not_dump_to_slow() {
        let cfg = SessionConfig::default();
        let last = mk_class(1, "a#0", 180, 180);
        let slow = mk_class(2, "s#0", 255, 255);
        assert!(
            failback_target(&[last.clone(), slow], &last, &cfg).is_none(),
            "last schedulable peer must not dump to slow"
        );
    }

    #[test]
    fn upgrade_slow_to_peer() {
        let cfg = SessionConfig::default();
        // 255 vs 180 is the far class-jump cliff (278ms); use 162 where
        // 255 ≥ 162×1.5+8 and upgrade is a class jump.
        let slow = mk_class(1, "s#0", 255, 255);
        let peer = mk_class(2, "a#0", 162, 162);
        let peer2 = mk_class(3, "b#0", 172, 172);
        let (to, reason) = failback_target(&[slow.clone(), peer, peer2], &slow, &cfg).unwrap();
        assert_eq!(to, 2);
        assert_eq!(reason, FailbackReason::Upgrade);
    }

    #[test]
    fn no_hop_to_slow_from_far_e() {
        let cfg = SessionConfig::default();
        let e = mk_class(1, "e#0", 198, 198);
        let a = mk_class(2, "a#0", 162, 162);
        let s1 = mk_class(3, "s1#0", 255, 255);
        let tgt = failback_target(&[e.clone(), a, s1], &e, &cfg);
        if let Some((id, _)) = tgt {
            assert_ne!(id, 3, "must not hop onto slow");
        }
    }

    #[test]
    fn interactive_prefers_empty_conn() {
        let cfg = SessionConfig::default();
        let a0 = mk_named(1, "a#0".into(), 12);
        let a1 = mk_named(2, "a#1".into(), 12);
        a0.inflight.store(128 * 1024, Ordering::Relaxed);
        let picked = pick_path_pref(&[a0, a1], &cfg, PickPref::Interactive).unwrap();
        assert_eq!(picked, 2);
    }

    #[test]
    fn all_elevated_does_not_dump_to_slow() {
        let cfg = SessionConfig::default();
        let a = mk_class(1, "a#0", 306, 180);
        let b = mk_class(2, "b#0", 306, 180);
        let slow = mk_class(3, "s#0", 255, 255);
        assert!(
            failback_target(&[a.clone(), b, slow], &a, &cfg).is_none(),
            "all peers 1.7× must not dump to slow"
        );
    }

    #[test]
    fn last_degraded_peer_does_not_dump_to_slow() {
        let cfg = SessionConfig::default();
        let last = mk_class(1, "a#0", 180, 180);
        last.state
            .store(crate::path::STATE_DEGRADED, Ordering::Relaxed);
        let slow = mk_class(2, "s#0", 255, 255);
        assert!(
            failback_target(&[last.clone(), slow], &last, &cfg).is_none(),
            "last DEGRADED peer must not dump to slow"
        );
    }

    #[test]
    fn degraded_cur_hops_class_empty_to_up_sibling() {
        let cfg = SessionConfig::default();
        let cur = mk_class(1, "a#0", 180, 180);
        cur.state
            .store(crate::path::STATE_DEGRADED, Ordering::Relaxed);
        let sib = mk_class(2, "b#0", 180, 180);
        let (to, reason) = failback_target(&[cur.clone(), sib], &cur, &cfg).unwrap();
        assert_eq!(to, 2);
        assert_eq!(reason, FailbackReason::ClassEmpty);
    }

    #[test]
    fn congested_cur_hops_class_empty_to_up_sibling() {
        let cfg = SessionConfig::default();
        let cur = mk_class(1, "a#0", 180, 180);
        cur.set_congested(true);
        let sib = mk_class(2, "b#0", 180, 180);
        let slow = mk_class(3, "s#0", 255, 255);
        let (to, reason) = failback_target(&[cur.clone(), sib, slow], &cur, &cfg).unwrap();
        assert_eq!(to, 2);
        assert_eq!(reason, FailbackReason::ClassEmpty);
    }

    #[test]
    fn two_degraded_peers_do_not_bounce_or_dump_to_slow() {
        let cfg = SessionConfig::default();
        let a = mk_class(1, "a#0", 180, 180);
        let b = mk_class(2, "b#0", 180, 180);
        a.state
            .store(crate::path::STATE_DEGRADED, Ordering::Relaxed);
        b.state
            .store(crate::path::STATE_DEGRADED, Ordering::Relaxed);
        let slow = mk_class(3, "s#0", 255, 255);
        assert!(failback_target(&[a.clone(), b.clone(), slow.clone()], &a, &cfg).is_none());
        assert!(backup_prefer_class(&[a.clone(), b, slow], 1, &cfg).is_none());
    }

    #[test]
    fn hol_fallback_never_picks_far_slow_from_182() {
        let cfg = SessionConfig::default();
        let cur = mk_class(1, "c#0", 182, 182);
        let s1 = mk_class(2, "s1#0", 255, 255);
        let dest = hol_place_bulk_fallback(&[cur.clone(), s1], &cur, &cfg, |id| id == 1);
        assert_ne!(dest, Some(2), "never s1 from 182, got {dest:?}");
        assert!(dest.is_none());
    }

    #[test]
    fn jitter_low_tail_does_not_singleton() {
        let cfg = SessionConfig::default();
        let classes = [162, 172, 182, 190, 198];
        let peers: Vec<_> = classes
            .iter()
            .enumerate()
            .map(|(i, &c)| {
                let p = mk_class(i as u32 + 1, &format!("p{i}"), c, c);
                if i == 0 {
                    p.stable_up_hold_us.store(1_000_000, Ordering::Relaxed);
                    for _ in 0..20 {
                        p.record_rtt(Duration::from_millis(90));
                    }
                    for _ in 0..5 {
                        p.record_rtt(Duration::from_millis(180));
                    }
                }
                p
            })
            .collect();
        let picked = pick_path(&peers, &cfg).unwrap();
        assert!(
            (1..=5).contains(&picked),
            "pick stays in the peer set, got {picked}"
        );
        assert!(
            failback_target(&peers, &peers[4], &cfg).is_none(),
            "198 vs 162–190 still same class after a low-tail on a"
        );
    }

    #[test]
    fn confirmed_2_5x_class_upgrades_to_unshifted_peer() {
        let cfg = SessionConfig::default();
        // 244 vs 162 is out of fastest class (Δ=82 ≥ 73) but not a class-jump
        // (244 ≱ 251). Healthy stickies stay; pick_path still prefers 162.
        let shifted = mk_class(1, "a#0", 450, 244);
        let peer = mk_class(2, "b#0", 162, 162);
        let slow = mk_class(3, "s#0", 255, 255);
        assert!(
            failback_target(
                &[shifted.clone(), peer.clone(), slow.clone()],
                &shifted,
                &cfg,
            )
            .is_none(),
            "244 vs 162 must not Upgrade (class-jump needs 251)"
        );
        let jumped = mk_class(4, "j#0", 280, 280);
        let peer2 = mk_class(5, "c#0", 172, 172);
        let (to, reason) =
            failback_target(&[jumped.clone(), peer, peer2, slow], &jumped, &cfg).unwrap();
        assert_eq!(to, 2);
        assert_eq!(reason, FailbackReason::Upgrade);
    }

    #[test]
    fn upgrade_slow_to_non_fastest_peer() {
        let cfg = SessionConfig::default();
        // High-band: slow 198 class-jumps from 124 (191) but not from 136 (212).
        // With two in-class peers, leave slow via 0.45. Alone, stay (Stay2).
        let slow = mk_class(1, "s#0", 198, 198);
        let b = mk_class(2, "b#0", 136, 136);
        let c = mk_class(3, "c#0", 148, 148);
        let (to, reason) = failback_target(&[slow.clone(), b.clone(), c], &slow, &cfg).unwrap();
        assert_eq!(to, 2);
        assert_eq!(reason, FailbackReason::Upgrade);
        assert!(
            failback_target(&[slow.clone(), b], &slow, &cfg).is_none(),
            "singleton fastest must not pull off slow (dump-to-slow bounce)"
        );
    }

    #[test]
    fn leave_slow_gates_on_fastest_min_not_loaded_best() {
        let cfg = SessionConfig::default();
        let slow = mk_class(1, "s#0", 258, 258);
        let a = mk_class(2, "a#0", 168, 168);
        a.sticky_streams.store(8, Ordering::Relaxed);
        a.inflight.store(200 * 1024, Ordering::Relaxed);
        let b = mk_class(3, "b#0", 182, 182);
        let (to, reason) = failback_target(&[slow.clone(), a, b], &slow, &cfg).unwrap();
        assert_ne!(to, 1);
        assert_eq!(reason, FailbackReason::Upgrade);
    }

    #[test]
    fn last_elevated_peer_does_not_upgrade_to_slow() {
        let cfg = SessionConfig::default();
        let last = mk_class(1, "a#0", 320, 320);
        let slow = mk_class(2, "s#0", 198, 198);
        assert!(
            failback_target(&[last.clone(), slow], &last, &cfg).is_none(),
            "last elevated peer vs singleton slow must stay"
        );
    }

    #[test]
    fn recovered_peer_stale_class_pulls_off_slow() {
        let cfg = SessionConfig::default();
        // Live 124ms, class still 320 after delay_shift. Two conns (production
        // default) so fastest.len()≥2. Must be in fastest_class via effective
        // RTT and pull stickies off 198ms slow.
        let a0 = mk_class(1, "a#0", 124, 320);
        let a1 = mk_class(2, "a#1", 126, 320);
        let slow = mk_class(3, "s#0", 198, 198);
        let paths = vec![a0.clone(), a1.clone(), slow.clone()];
        let picked = pick_path(&paths, &cfg).unwrap();
        assert_ne!(picked, 3, "recovered peer must win pick vs slow");
        let (to, reason) = failback_target(&paths, &slow, &cfg).unwrap();
        assert_ne!(to, 3);
        assert_eq!(reason, FailbackReason::Upgrade);
        assert!(
            failback_target(&paths, &a0, &cfg).is_none(),
            "live-fast recovered peer must not dump onto slow"
        );
        assert_eq!(effective_class_rtt(&cfg, &a0), Duration::from_millis(124));
    }

    #[test]
    fn delay_spike_fast_does_not_become_effective_class() {
        let cfg = SessionConfig::default();
        let spiked = mk_class(1, "a#0", 306, 180);
        assert_eq!(
            effective_class_rtt(&cfg, &spiked),
            Duration::from_millis(180),
            "fast >> class is a spike, keep class"
        );
    }

    #[test]
    fn in_fastest_class_peer_does_not_upgrade_on_0_45() {
        let cfg = SessionConfig::default();
        let c = mk_class(1, "c#0", 148, 148);
        let a = mk_class(2, "a#0", 124, 124);
        assert!(
            failback_target(&[c.clone(), a], &c, &cfg).is_none(),
            "148 vs 124 is same fastest class; no Upgrade"
        );
    }

    #[test]
    fn backup_prefer_class_takes_same_link_sibling() {
        let cfg = SessionConfig::default();
        let a0 = mk_class(1, "a#0", 12, 12);
        a0.state
            .store(crate::path::STATE_DEGRADED, Ordering::Relaxed);
        let a1 = mk_class(2, "a#1", 13, 13);
        assert_eq!(backup_prefer_class(&[a0, a1], 1, &cfg), Some(2));
    }

    #[test]
    fn unfrozen_class_stays_in_fastest_and_does_not_upgrade() {
        let cfg = SessionConfig::default();
        // a has no class yet (reconnect / init). b is a frozen 96ms mid peer.
        // Must not treat a's fast EWMA as a singleton dest, and a must not
        // Upgrade-away on jitter during the 8-sample window.
        let a = mk_named(1, "a#0".into(), 50);
        assert!(!a.class_known());
        let b = mk_class(2, "b#0", 96, 96);
        let slow = mk_class(3, "s#0", 125, 125);
        let paths = vec![a.clone(), b.clone(), slow.clone()];
        let picked = pick_path(&paths, &cfg).unwrap();
        assert_ne!(picked, 3, "unfrozen peer must not lose to slow");
        assert!(
            failback_target(&paths, &a, &cfg).is_none(),
            "unfrozen class stays in fastest; no Upgrade"
        );
        assert!(
            failback_target(&paths, &b, &cfg).is_none(),
            "must not Upgrade onto an unfrozen dest"
        );
    }

    #[test]
    fn backup_prefer_class_same_link_despite_unknown_rtt_class_jump() {
        let cfg = SessionConfig::default();
        let a1 = mk_named(2, "a#1".into(), 0);
        a1.rtt_ewma_us.store(101, Ordering::Relaxed);
        a1.rtt_class_us.store(101, Ordering::Relaxed);
        a1.set_congested(true);
        let a0 = mk_named(1, "a#0".into(), 0); // unknown → 20ms fallback
        assert_eq!(backup_prefer_class(&[a1, a0], 2, &cfg), Some(1));
    }

    #[test]
    fn unknown_not_in_fastest_class_with_7ms_peers() {
        let cfg = SessionConfig::default();
        let known = mk_named(1, "a#0".into(), 7);
        let unk = mk_named(2, "b#0".into(), 0);
        assert!(!unk.rtt_known());
        let ids: Vec<u32> = fastest_class_set(&[known.clone(), unk.clone()], &cfg)
            .iter()
            .map(|p| p.id)
            .collect();
        assert_eq!(ids, vec![1]);
        assert_eq!(pick_path(&[known, unk], &cfg), Some(1));
    }

    #[test]
    fn unknown_not_in_fastest_class_with_13ms_peers() {
        let cfg = SessionConfig::default();
        let known = mk_named(1, "a#0".into(), 13);
        let unk = mk_named(2, "b#0".into(), 0);
        assert_eq!(pick_path(&[known, unk], &cfg), Some(1));
    }

    #[test]
    fn unknown_picked_when_known_are_degraded() {
        let cfg = SessionConfig::default();
        let known = mk_named(1, "a#0".into(), 7);
        known
            .state
            .store(crate::path::STATE_DEGRADED, Ordering::Relaxed);
        let unk = mk_named(2, "b#0".into(), 0);
        assert_eq!(pick_path(&[known, unk], &cfg), Some(2));
    }

    #[test]
    fn all_unknown_still_picked() {
        let cfg = SessionConfig::default();
        let a = mk_named(1, "a#0".into(), 0);
        let b = mk_named(2, "b#0".into(), 0);
        assert!(pick_path(&[a, b], &cfg).is_some());
    }
}
