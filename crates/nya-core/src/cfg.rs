use std::time::Duration;

use serde::Deserialize;

use crate::tuning::Tuning;

/// Operator-facing probe budget, path cap, and give-up timer.
///
/// Algorithm / health / failback formula live in [`Tuning`]
/// (`SessionConfig.tuning`). TOML cannot set those.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub ping_interval_min: Duration,
    pub ping_interval_max: Duration,
    pub all_down_timeout: Duration,
    pub max_paths: usize,
    /// Implementation knobs. Not deserialized from TOML.
    pub tuning: Tuning,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            ping_interval_min: Duration::from_millis(10),
            ping_interval_max: Duration::from_millis(50),
            all_down_timeout: Duration::from_secs(8),
            max_paths: 32,
            tuning: Tuning::STANDARD,
        }
    }
}

/// Optional overrides from TOML. Missing fields keep [`SessionConfig::default`].
/// Unknown keys under `[session]` are a parse error (no aliases).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOpts {
    pub ping_interval_min_ms: Option<u64>,
    pub ping_interval_max_ms: Option<u64>,
    pub all_down_timeout_ms: Option<u64>,
    pub max_paths: Option<usize>,
}

impl SessionOpts {
    pub fn apply(&self, mut c: SessionConfig) -> SessionConfig {
        fn ms(v: u64) -> Duration {
            Duration::from_millis(v)
        }
        if let Some(v) = self.ping_interval_min_ms {
            c.ping_interval_min = ms(v);
        }
        if let Some(v) = self.ping_interval_max_ms {
            c.ping_interval_max = ms(v);
        }
        if let Some(v) = self.all_down_timeout_ms {
            c.all_down_timeout = ms(v);
        }
        if let Some(v) = self.max_paths {
            c.max_paths = v.max(1);
        }
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_opts_from_file(raw: &str) -> SessionOpts {
        let v: toml::Value = toml::from_str(raw).expect("toml");
        let session = v
            .get("session")
            .cloned()
            .unwrap_or(toml::Value::Table(Default::default()));
        session.try_into().expect("SessionOpts")
    }

    #[test]
    fn empty_opts_keep_defaults() {
        let c = SessionOpts::default().apply(SessionConfig::default());
        let d = SessionConfig::default();
        assert_eq!(c.ping_interval_min, d.ping_interval_min);
        assert_eq!(c.ping_interval_max, d.ping_interval_max);
        assert_eq!(c.all_down_timeout, d.all_down_timeout);
        assert_eq!(c.max_paths, d.max_paths);
        assert_eq!(c.tuning, Tuning::STANDARD);
    }

    #[test]
    fn four_keys_apply_and_max_paths_at_least_one() {
        let opts = SessionOpts {
            ping_interval_min_ms: Some(15),
            ping_interval_max_ms: Some(40),
            all_down_timeout_ms: Some(4000),
            max_paths: Some(0),
        };
        let c = opts.apply(SessionConfig::default());
        assert_eq!(c.ping_interval_min, Duration::from_millis(15));
        assert_eq!(c.ping_interval_max, Duration::from_millis(40));
        assert_eq!(c.all_down_timeout, Duration::from_millis(4000));
        assert_eq!(c.max_paths, 1);
        assert_eq!(c.tuning, Tuning::STANDARD);
    }

    #[test]
    fn leftover_algorithm_keys_are_parse_errors() {
        for raw in [
            "failback_abs_frac = 0.45",
            "down_timeout_floor_ms = 50",
            "failback = true",
            "loss_timeout_mult = 2.0",
        ] {
            let err = toml::from_str::<SessionOpts>(raw).expect_err(raw);
            let msg = err.to_string();
            assert!(
                msg.contains("unknown field") || msg.contains("did you mean"),
                "{raw}: {msg}"
            );
        }
    }

    #[test]
    fn examples_session_tables_deserialize() {
        for (name, raw) in [
            ("client", include_str!("../../../examples/client.toml")),
            ("server", include_str!("../../../examples/server.toml")),
        ] {
            let opts = session_opts_from_file(raw);
            let _ = opts.apply(SessionConfig::default());
            let _ = name;
        }
    }
}
