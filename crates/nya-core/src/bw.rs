//! Windowed max filter for ACK-clock bandwidth samples (BBR's `minmax`).
//!
//! Three (value, time) slots. The best sample is slot 0; slots 1 and 2 hold
//! the best samples seen since slot 0 and since slot 1 respectively, so when
//! slot 0 ages out of the window the next-best recent sample takes over
//! without rescanning history. Pure data structure; time is caller-supplied
//! monotonic microseconds.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Slot {
    v: u64,
    t: u64,
}

/// Windowed maximum over the last `win_us` of samples.
#[derive(Clone, Debug, Default)]
pub struct MinMax3 {
    s: [Slot; 3],
}

impl MinMax3 {
    pub const fn new() -> Self {
        Self {
            s: [Slot { v: 0, t: 0 }; 3],
        }
    }

    /// Current windowed max, or 0 when no sample is younger than `win_us`
    /// (slot 2 always holds the newest sample). An old max is still the
    /// answer while younger samples exist: sliding it out is `update`'s
    /// job, exactly as in BBR's `minmax_get`.
    pub fn get(&self, now_us: u64, win_us: u64) -> u64 {
        if self.s[0].t == 0 || now_us.saturating_sub(self.s[2].t) > win_us {
            return 0;
        }
        self.s[0].v
    }

    fn reset(&mut self, v: u64, t: u64) {
        self.s = [Slot { v, t }; 3];
    }

    /// Insert a sample at time `t_us` with window `win_us`. Returns the new max.
    pub fn update(&mut self, v: u64, t_us: u64, win_us: u64) -> u64 {
        let t_us = t_us.max(1);
        let new = Slot { v, t: t_us };
        // New global max, or the whole history is stale: restart.
        if self.s[0].t == 0 || v >= self.s[0].v || t_us.saturating_sub(self.s[2].t) > win_us {
            self.reset(v, t_us);
            return v;
        }
        if v >= self.s[1].v {
            self.s[1] = new;
            self.s[2] = new;
        } else if v >= self.s[2].v {
            self.s[2] = new;
        }
        self.expire(new, win_us);
        self.s[0].v
    }

    /// Slide the window: drop slot 0 once it is older than `win_us`,
    /// promoting the best of the younger slots (BBR `minmax_subwin_update`).
    fn expire(&mut self, new: Slot, win_us: u64) {
        let dt = new.t.saturating_sub(self.s[0].t);
        if dt > win_us {
            self.s[0] = self.s[1];
            self.s[1] = self.s[2];
            self.s[2] = new;
            if new.t.saturating_sub(self.s[0].t) > win_us {
                self.s[0] = self.s[1];
                self.s[1] = self.s[2];
                self.s[2] = new;
            }
        } else if self.s[1].t == self.s[0].t && dt > win_us / 4 {
            // Passed a quarter of the window without a challenger: the
            // second slot must be a fresh candidate, not a copy of the max.
            self.s[2] = new;
            self.s[1] = new;
        } else if self.s[2].t == self.s[1].t && dt > win_us / 2 {
            self.s[2] = new;
        }
    }
}

/// Windowed minimum built on [`MinMax3`] by storing `u64::MAX - v`.
#[derive(Clone, Debug, Default)]
pub struct WindowedMin(MinMax3);

impl WindowedMin {
    pub const fn new() -> Self {
        Self(MinMax3::new())
    }

    /// Current windowed min, or `None` when no sample is younger than `win_us`.
    pub fn get(&self, now_us: u64, win_us: u64) -> Option<u64> {
        if self.0.s[0].t == 0 || now_us.saturating_sub(self.0.s[2].t) > win_us {
            return None;
        }
        Some(u64::MAX - self.0.s[0].v)
    }

    pub fn update(&mut self, v: u64, t_us: u64, win_us: u64) -> u64 {
        u64::MAX - self.0.update(u64::MAX - v, t_us, win_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WIN: u64 = 1_000_000;

    #[test]
    fn windowed_min_tracks_low_and_expires() {
        let mut m = WindowedMin::new();
        assert_eq!(m.get(1, WIN), None);
        m.update(30_000, 1, WIN);
        m.update(10_000, 100_000, WIN);
        assert_eq!(m.get(300_000, WIN), Some(10_000));
        // Queueing-inflated samples never raise a fresh min.
        for i in 0..20u64 {
            m.update(50_000, 300_000 + i * 10_000, WIN);
        }
        assert_eq!(m.get(500_000, WIN), Some(10_000));
        // Once the 10 ms sample ages out, a younger candidate takes over
        // (BBR minmax semantics: the best challenger seen after the quarter
        // window, not a rescan).
        let r = m.update(45_000, 1_200_000, WIN);
        assert!(r > 10_000 && r <= 50_000, "{r}");
        assert_eq!(m.get(1_200_000, WIN), Some(r));
        // A lower sample is adopted immediately.
        assert_eq!(m.update(12_000, 1_300_000, WIN), 12_000);
        // Silence past the window: nothing fresh.
        assert_eq!(m.get(2_400_000, WIN), None);
    }

    #[test]
    fn empty_is_zero() {
        let m = MinMax3::new();
        assert_eq!(m.get(10, WIN), 0);
    }

    #[test]
    fn rising_samples_track_max() {
        let mut m = MinMax3::new();
        for (i, v) in [10u64, 20, 30, 40].iter().enumerate() {
            m.update(*v, 1 + i as u64 * 1000, WIN);
        }
        assert_eq!(m.get(5000, WIN), 40);
    }

    #[test]
    fn max_expires_after_window() {
        let mut m = MinMax3::new();
        m.update(100, 1, WIN);
        m.update(50, 300_000, WIN);
        m.update(40, 600_000, WIN);
        assert_eq!(m.get(900_000, WIN), 100);
        // A sample past the window drops the 100 and promotes the 50.
        assert_eq!(m.update(30, 1_100_000, WIN), 50);
    }

    #[test]
    fn idle_past_window_reads_zero_then_restarts() {
        let mut m = MinMax3::new();
        m.update(100, 1, WIN);
        assert_eq!(m.get(2_000_000, WIN), 0);
        assert_eq!(m.update(5, 3_000_000, WIN), 5);
    }

    #[test]
    fn lower_sample_never_lowers_fresh_max() {
        let mut m = MinMax3::new();
        m.update(100, 1, WIN);
        for i in 1..50u64 {
            m.update(1, 1 + i * 10_000, WIN);
        }
        assert_eq!(m.get(500_000, WIN), 100);
    }
}
