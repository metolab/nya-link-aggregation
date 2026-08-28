use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use crate::tuning::Tuning;

/// Operator-facing observability. Log verbosity stays on `RUST_LOG`.
/// `Eq` is omitted because nested `sample_ratio` is `f64`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObsOpts {
    /// `None` → 10s. `Some(0)` → disable the periodic snapshot.
    pub snapshot_interval_ms: Option<u64>,
    /// Empty / `None` → do not listen. Must be a numeric loopback `SocketAddr`.
    pub metrics_listen: Option<String>,
    /// Operator instance name. Required by `nya_obs::install` when OTel is on.
    pub instance_name: Option<String>,
    #[serde(default)]
    pub otel: OtelOpts,
}

/// Remote OTLP export. Default-off. Nested under `[obs.otel]`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OtelOpts {
    /// Master switch. `false` / missing = all signals off.
    #[serde(default)]
    pub enabled: bool,
    pub endpoint: Option<String>,
    pub protocol: Option<OtelProtocol>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub environment: Option<String>,
    /// Metrics PeriodicReader interval. Default 10000.
    pub export_interval_ms: Option<u64>,
    /// OTLP export timeout and traces/logs shutdown flush. Default 5000.
    /// Metrics PeriodicReader has no collection timeout in the 0.31 SDK.
    pub timeout_ms: Option<u64>,
    /// Default true.
    pub gzip: Option<bool>,
    #[serde(default)]
    pub traces: OtelSignalOpts,
    #[serde(default)]
    pub metrics: OtelSignalOpts,
    #[serde(default)]
    pub logs: OtelSignalOpts,
    /// Trace sample ratio. Default 1.0. Rejected outside [0.0, 1.0] at install.
    pub sample_ratio: Option<f64>,
    /// Logs only: rewrite host/target attributes to `*`.
    #[serde(default)]
    pub redact_targets: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OtelProtocol {
    #[default]
    #[serde(rename = "http/protobuf")]
    HttpProtobuf,
    #[serde(rename = "grpc")]
    Grpc,
}

/// Per-signal overrides under `[obs.otel.{traces,metrics,logs}]`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OtelSignalOpts {
    /// `None` follows parent `obs.otel.enabled`.
    pub enabled: Option<bool>,
    pub endpoint: Option<String>,
    /// Logs only: `error`/`warn`/`info`/`debug`/`trace`.
    pub level: Option<String>,
    /// Logs and traces: BatchLog/SpanProcessor max queue.
    pub queue_size: Option<u32>,
    /// Logs and traces: max export batch size.
    pub batch_size: Option<u32>,
    /// Logs and traces: scheduled delay in milliseconds.
    pub delay_ms: Option<u64>,
}

impl ObsOpts {
    /// `None` → 10s. `Some(0)` → disabled.
    pub fn snapshot_interval(&self) -> Option<Duration> {
        match self.snapshot_interval_ms {
            None => Some(Duration::from_millis(10_000)),
            Some(0) => None,
            Some(ms) => Some(Duration::from_millis(ms)),
        }
    }

    /// `None` / `""` → None. Other values are passed to bind (numeric
    /// `SocketAddr` + loopback check happens at spawn).
    pub fn metrics_listen(&self) -> Option<&str> {
        match self.metrics_listen.as_deref() {
            None | Some("") => None,
            Some(s) => Some(s),
        }
    }
}

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
    fn obs_opts_unknown_key_is_error() {
        let err = toml::from_str::<ObsOpts>("json = true").expect_err("unknown");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") || msg.contains("did you mean"),
            "{msg}"
        );
    }

    #[test]
    fn obs_opts_interval_none_is_10s_zero_disables() {
        let d = ObsOpts::default();
        assert_eq!(d.snapshot_interval(), Some(Duration::from_millis(10_000)));
        assert!(d.metrics_listen().is_none());
        let off = ObsOpts {
            snapshot_interval_ms: Some(0),
            metrics_listen: Some(String::new()),
            ..Default::default()
        };
        assert!(off.snapshot_interval().is_none());
        assert!(off.metrics_listen().is_none());
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

    #[test]
    fn otel_nested_table_deserializes_and_sample_ratio_is_f64() {
        let o: ObsOpts = toml::from_str(
            r#"
instance_name = "edge-sh-03"
[otel]
enabled = true
endpoint = "http://127.0.0.1:4318"
protocol = "http/protobuf"
sample_ratio = 1.0
[otel.logs]
level = "info"
queue_size = 8192
batch_size = 512
delay_ms = 5000
"#,
        )
        .expect("otel opts");
        assert_eq!(o.instance_name.as_deref(), Some("edge-sh-03"));
        assert!(o.otel.enabled);
        assert_eq!(o.otel.sample_ratio, Some(1.0));
        assert_eq!(o.otel.logs.level.as_deref(), Some("info"));
        assert_eq!(o.otel.logs.queue_size, Some(8192));
    }

    #[test]
    fn otel_flat_endpoint_metrics_is_unknown_field() {
        let err = toml::from_str::<ObsOpts>(
            r#"
[otel]
enabled = true
endpoint_metrics = "http://127.0.0.1:4318"
"#,
        )
        .expect_err("flat key");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") || msg.contains("did you mean"),
            "{msg}"
        );
    }

    #[test]
    fn enabled_false_may_carry_other_keys() {
        let o: ObsOpts = toml::from_str(
            r#"
[otel]
enabled = false
endpoint = "http://127.0.0.1:4318"
"#,
        )
        .expect("disabled with endpoint");
        assert!(!o.otel.enabled);
        assert!(o.otel.endpoint.is_some());
    }
}
