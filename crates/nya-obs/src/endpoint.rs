//! HTTP OTLP URL join. `opentelemetry-otlp` 0.31 `with_endpoint` is a full
//! signal URL and does not append `/v1/{signal}`.

use anyhow::{bail, Result};
use nya_core::{OtelOpts, OtelProtocol, OtelSignalOpts};

const SIGNAL_PATHS: [&str; 3] = ["/v1/traces", "/v1/metrics", "/v1/logs"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EndpointKind {
    /// Parent `[obs.otel].endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT`.
    Base,
    /// `[obs.otel.{traces,metrics,logs}].endpoint`.
    Signal,
}

pub(crate) fn resolve_endpoint_kind(
    otel: &OtelOpts,
    sig: Option<&OtelSignalOpts>,
) -> (String, EndpointKind) {
    if let Some(sig) = sig {
        if let Some(e) = sig
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return (e.to_string(), EndpointKind::Signal);
        }
    }
    if let Some(e) = otel
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return (e.to_string(), EndpointKind::Base);
    }
    let env = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_default();
    (env, EndpointKind::Base)
}

/// Full URL passed to `with_endpoint` for one HTTP signal.
pub(crate) fn http_signal_url(
    raw: &str,
    signal: &'static str,
    kind: EndpointKind,
) -> Result<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("empty OTLP endpoint");
    }
    if !(raw.starts_with("http://") || raw.starts_with("https://")) {
        bail!("OTLP HTTP endpoint must start with http:// or https://");
    }
    let (base, query) = split_query_fragment(raw);
    let want = match signal {
        "traces" | "metrics" | "logs" => format!("/v1/{signal}"),
        _ => bail!("unknown OTLP signal {signal}"),
    };
    let trimmed = base.trim_end_matches('/');
    if let Some(existing) = known_signal_suffix(trimmed) {
        match kind {
            EndpointKind::Signal => {
                return Ok(format!("{trimmed}{query}"));
            }
            EndpointKind::Base => {
                let stripped = trimmed
                    .strip_suffix(existing)
                    .unwrap_or(trimmed)
                    .trim_end_matches('/');
                return Ok(join_slash(stripped, &want) + query);
            }
        }
    }
    if !query.is_empty() {
        bail!("query/fragment only allowed on a full /v1/{{signal}} HTTP URL");
    }
    Ok(join_slash(base, &want))
}

fn known_signal_suffix(url: &str) -> Option<&'static str> {
    SIGNAL_PATHS.iter().copied().find(|p| url.ends_with(p))
}

fn join_slash(base: &str, path: &str) -> String {
    if base.ends_with('/') && path.starts_with('/') {
        format!("{base}{}", &path[1..])
    } else if !base.ends_with('/') && !path.starts_with('/') {
        format!("{base}/{path}")
    } else {
        format!("{base}{path}")
    }
}

fn split_query_fragment(s: &str) -> (&str, &str) {
    match s.find(['?', '#']) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    }
}

pub(crate) fn exporter_url(
    otel: &OtelOpts,
    sig: &OtelSignalOpts,
    protocol: OtelProtocol,
    signal: &'static str,
) -> Result<String> {
    let (raw, kind) = resolve_endpoint_kind(otel, Some(sig));
    if raw.is_empty() {
        bail!(
            "obs.otel.endpoint, obs.otel.{signal}.endpoint, or OTEL_EXPORTER_OTLP_ENDPOINT required when this signal is enabled"
        );
    }
    match protocol {
        OtelProtocol::HttpProtobuf => http_signal_url(&raw, signal, kind),
        OtelProtocol::Grpc => Ok(raw.trim().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(raw: &str, signal: &'static str, kind: EndpointKind) -> Result<String> {
        http_signal_url(raw, signal, kind)
    }

    #[test]
    fn join_table() {
        use EndpointKind::{Base, Signal};
        let rows: &[(&str, EndpointKind, &str, &str)] = &[
            (
                "https://otlp.srv.akalyn.cn",
                Base,
                "traces",
                "https://otlp.srv.akalyn.cn/v1/traces",
            ),
            (
                "https://otlp.srv.akalyn.cn/",
                Base,
                "logs",
                "https://otlp.srv.akalyn.cn/v1/logs",
            ),
            (
                "http://127.0.0.1:4318",
                Base,
                "metrics",
                "http://127.0.0.1:4318/v1/metrics",
            ),
            (
                "https://host:4318/v1/traces",
                Base,
                "traces",
                "https://host:4318/v1/traces",
            ),
            (
                "https://host:4318/v1/traces",
                Base,
                "metrics",
                "https://host:4318/v1/metrics",
            ),
            (
                "https://host:4318/v1/traces/",
                Base,
                "traces",
                "https://host:4318/v1/traces",
            ),
            (
                "https://host:4318/otlp",
                Base,
                "traces",
                "https://host:4318/otlp/v1/traces",
            ),
            (
                "https://host:4318/v1/traces",
                Signal,
                "traces",
                "https://host:4318/v1/traces",
            ),
            (
                "https://host:4318",
                Signal,
                "traces",
                "https://host:4318/v1/traces",
            ),
            (
                "https://host:4318/v1/metrics",
                Signal,
                "traces",
                "https://host:4318/v1/metrics",
            ),
            (
                "https://host:4318/v1/traces?foo=1",
                Signal,
                "traces",
                "https://host:4318/v1/traces?foo=1",
            ),
        ];
        for (raw, kind, signal, want) in rows {
            let got = http(raw, signal, *kind).unwrap();
            assert_eq!(got, *want, "in={raw} {kind:?} {signal}");
        }
        assert!(http("https://host:4318?foo=1", "traces", Base).is_err());
        assert!(http("https://host:4318?foo=1", "traces", Signal).is_err());
        assert!(http("otlp.srv.akalyn.cn", "traces", Base).is_err());
        assert!(http("", "traces", Base).is_err());
    }

    #[test]
    fn grpc_exporter_url_is_verbatim() {
        let otel = OtelOpts {
            endpoint: Some("https://host:4317".into()),
            ..Default::default()
        };
        let url = exporter_url(&otel, &otel.traces, OtelProtocol::Grpc, "traces").unwrap();
        assert_eq!(url, "https://host:4317");
        let otel = OtelOpts {
            endpoint: Some("https://host:4317/v1/traces".into()),
            ..Default::default()
        };
        let url = exporter_url(&otel, &otel.traces, OtelProtocol::Grpc, "traces").unwrap();
        assert_eq!(url, "https://host:4317/v1/traces");
    }

    #[test]
    fn parent_empty_per_signal_traces() {
        let otel = OtelOpts {
            traces: OtelSignalOpts {
                endpoint: Some("http://127.0.0.1:4318/v1/traces".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let url = exporter_url(&otel, &otel.traces, OtelProtocol::HttpProtobuf, "traces").unwrap();
        assert_eq!(url, "http://127.0.0.1:4318/v1/traces");
        let err = exporter_url(&otel, &otel.metrics, OtelProtocol::HttpProtobuf, "metrics")
            .unwrap_err()
            .to_string();
        assert!(err.contains("endpoint"), "{err}");
    }

    #[test]
    fn per_signal_wins_over_parent() {
        let otel = OtelOpts {
            endpoint: Some("http://parent:4318".into()),
            traces: OtelSignalOpts {
                endpoint: Some("http://sig:4318/v1/traces".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let (raw, kind) = resolve_endpoint_kind(&otel, Some(&otel.traces));
        assert_eq!(raw, "http://sig:4318/v1/traces");
        assert_eq!(kind, EndpointKind::Signal);
        let (raw, kind) = resolve_endpoint_kind(&otel, Some(&otel.metrics));
        assert_eq!(raw, "http://parent:4318");
        assert_eq!(kind, EndpointKind::Base);
    }
}
