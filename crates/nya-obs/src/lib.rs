//! OpenTelemetry export layer. SDK stays here and in binary `main.rs` only.
#![allow(clippy::type_complexity)]

mod endpoint;
mod metrics_export;
mod resource;
mod subscribe;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use nya_core::{
    ObsOpts, OtelOpts, OtelProtocol, OtelSignalOpts, ProcessSnapshot, Session, SessionTable,
};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::info;

pub use resource::build_resource;

pub const NAMESPACE: &str = "nya-link-aggregation";

#[derive(Debug)]
pub struct OtelGuard {
    enabled: bool,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        let _ = shutdown_inner(self.enabled);
    }
}

impl OtelGuard {
    pub fn shutdown(self) {
        let enabled = self.enabled;
        std::mem::forget(self);
        let _ = shutdown_inner(enabled);
    }
}

struct OtelRuntime {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
    logger: Option<SdkLoggerProvider>,
    timeout: Duration,
    metrics_on: bool,
}

static RUNTIME: Mutex<Option<OtelRuntime>> = Mutex::new(None);

fn runtime_slot() -> std::sync::MutexGuard<'static, Option<OtelRuntime>> {
    RUNTIME.lock().unwrap_or_else(|e| e.into_inner())
}

/// Install fmt (always) and OTLP (when enabled). `role` is `"client"` or `"server"`.
pub fn install(role: &'static str, version: &'static str, obs: &ObsOpts) -> Result<OtelGuard> {
    if runtime_slot().is_some() {
        bail!("otel already installed");
    }

    let disabled = std::env::var("OTEL_SDK_DISABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let enabled = obs.otel.enabled && !disabled;

    if !enabled {
        subscribe::init_fmt_only(role)?;
        info!("otel disabled");
        return Ok(OtelGuard { enabled: false });
    }

    validate_signal_keys(&obs.otel)?;
    let instance = resolve_instance_name(obs)?;
    let run_id = resource::new_run_id();
    let protocol = resolve_protocol(&obs.otel)?;
    if matches!(protocol, OtelProtocol::Grpc) && !cfg!(feature = "grpc") {
        bail!("rebuild with --features otel-grpc");
    }

    let traces_on = signal_on(&obs.otel, &obs.otel.traces);
    let metrics_on = signal_on(&obs.otel, &obs.otel.metrics);
    let logs_on = signal_on(&obs.otel, &obs.otel.logs);
    let traces_url = if traces_on {
        Some(endpoint::exporter_url(
            &obs.otel,
            &obs.otel.traces,
            protocol,
            "traces",
        )?)
    } else {
        None
    };
    let metrics_url = if metrics_on {
        Some(endpoint::exporter_url(
            &obs.otel,
            &obs.otel.metrics,
            protocol,
            "metrics",
        )?)
    } else {
        None
    };
    let logs_url = if logs_on {
        Some(endpoint::exporter_url(
            &obs.otel,
            &obs.otel.logs,
            protocol,
            "logs",
        )?)
    } else {
        None
    };
    let (raw_parent, _) = endpoint::resolve_endpoint_kind(&obs.otel, None);

    let timeout = Duration::from_millis(obs.otel.timeout_ms.unwrap_or(5000).max(1));
    let gzip = obs.otel.gzip.unwrap_or(true);
    let headers = merge_headers(&obs.otel);
    let resource = build_resource(role, version, &instance, &run_id, obs);
    let ratio = obs.otel.sample_ratio.unwrap_or(1.0);
    if !(0.0..=1.0).contains(&ratio) {
        bail!("obs.otel.sample_ratio must be in [0.0, 1.0]");
    }

    let tracer = if traces_on {
        Some(build_tracer(
            &resource,
            traces_url.as_deref().unwrap(),
            protocol,
            gzip,
            &headers,
            timeout,
            &obs.otel.traces,
            ratio,
        )?)
    } else {
        None
    };
    let meter = if metrics_on {
        Some(build_meter(
            &resource,
            metrics_url.as_deref().unwrap(),
            protocol,
            gzip,
            &headers,
            timeout,
            Duration::from_millis(obs.otel.export_interval_ms.unwrap_or(10_000).max(1)),
        )?)
    } else {
        None
    };
    let logger = if logs_on {
        Some(build_logger(
            &resource,
            logs_url.as_deref().unwrap(),
            protocol,
            gzip,
            &headers,
            timeout,
            &obs.otel.logs,
        )?)
    } else {
        None
    };

    let log_level = if logs_on {
        Some(parse_log_level(obs.otel.logs.level.as_deref())?)
    } else {
        None
    };

    subscribe::init_otel(
        role,
        tracer.as_ref(),
        logger.as_ref(),
        traces_on,
        log_level,
        obs.otel.redact_targets,
    )?;

    if let Some(ref m) = meter {
        opentelemetry::global::set_meter_provider(m.clone());
    }
    if let Some(ref t) = tracer {
        opentelemetry::global::set_tracer_provider(t.clone());
    }

    *runtime_slot() = Some(OtelRuntime {
        tracer,
        meter,
        logger,
        timeout,
        metrics_on,
    });

    let parent_log = if raw_parent.trim().is_empty() {
        "-"
    } else {
        raw_parent.as_str()
    };
    info!(
        instance_name = %instance,
        run_id = %run_id,
        endpoint = parent_log,
        traces_endpoint = traces_url.as_deref().unwrap_or("-"),
        metrics_endpoint = metrics_url.as_deref().unwrap_or("-"),
        logs_endpoint = logs_url.as_deref().unwrap_or("-"),
        protocol = ?protocol,
        traces = traces_on,
        metrics = metrics_on,
        logs = logs_on,
        "otel enabled"
    );
    Ok(OtelGuard { enabled: true })
}

pub fn try_attach_session(session: &Session) {
    let session = session.clone();
    attach_source(Arc::new(move || ProcessSnapshot {
        process: session.process().snap(),
        session: session.snapshot(),
        session_fps: session.session_fp().into_iter().collect(),
    }));
}

pub fn try_attach_table(table: &Arc<SessionTable>) {
    let table = table.clone();
    attach_source(Arc::new(move || table.aggregate_snapshot()));
}

fn attach_source(src: Arc<dyn Fn() -> ProcessSnapshot + Send + Sync>) {
    let slot = runtime_slot();
    let Some(rt) = slot.as_ref() else {
        return;
    };
    if !rt.metrics_on {
        return;
    }
    drop(slot);
    metrics_export::register(src);
}

fn shutdown_inner(enabled: bool) -> Result<()> {
    let taken = runtime_slot().take();
    subscribe::reset_filters();
    let Some(rt) = taken else {
        return Ok(());
    };
    if !enabled {
        return Ok(());
    }
    let timeout = rt.timeout;
    if let Some(p) = rt.tracer {
        let _ = p.shutdown_with_timeout(timeout);
    }
    if let Some(p) = rt.meter {
        let _ = p.shutdown_with_timeout(timeout);
    }
    if let Some(p) = rt.logger {
        let _ = p.shutdown_with_timeout(timeout);
    }
    Ok(())
}

fn signal_on(parent: &OtelOpts, sig: &OtelSignalOpts) -> bool {
    parent.enabled && sig.enabled.unwrap_or(true)
}

fn validate_signal_keys(otel: &OtelOpts) -> Result<()> {
    if otel.metrics.level.is_some() {
        bail!("obs.otel.metrics.level is not valid (level is logs-only)");
    }
    if otel.traces.level.is_some() {
        bail!("obs.otel.traces.level is not valid (level is logs-only)");
    }
    for (name, sig) in [
        ("metrics", &otel.metrics),
        ("traces", &otel.traces),
        ("logs", &otel.logs),
    ] {
        if name == "metrics"
            && (sig.queue_size.is_some() || sig.batch_size.is_some() || sig.delay_ms.is_some())
        {
            bail!("obs.otel.metrics does not take queue_size/batch_size/delay_ms");
        }
    }
    Ok(())
}

fn resolve_instance_name(obs: &ObsOpts) -> Result<String> {
    let toml = obs
        .instance_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let env = std::env::var("NYA_INSTANCE_NAME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    match (toml, env) {
        (Some(s), _) => Ok(s.to_string()),
        (None, Some(s)) => Ok(s),
        (None, None) => bail!("obs.instance_name or NYA_INSTANCE_NAME required when otel.enabled"),
    }
}

fn resolve_protocol(otel: &OtelOpts) -> Result<OtelProtocol> {
    if let Some(p) = otel.protocol {
        return Ok(p);
    }
    match std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        None | Some("") => Ok(OtelProtocol::HttpProtobuf),
        Some("http/protobuf") | Some("http") => Ok(OtelProtocol::HttpProtobuf),
        Some("grpc") => Ok(OtelProtocol::Grpc),
        Some(other) => bail!("unknown OTEL_EXPORTER_OTLP_PROTOCOL {other}"),
    }
}

fn merge_headers(otel: &OtelOpts) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    if let Ok(raw) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
        for part in raw.split(',') {
            if let Some((k, v)) = part.split_once('=') {
                out.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    for (k, v) in &otel.headers {
        out.insert(k.clone(), v.clone());
    }
    out
}

fn parse_log_level(raw: Option<&str>) -> Result<tracing::Level> {
    let owned = std::env::var("NYA_OTEL_LOG_LEVEL").ok();
    let s = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or(owned.as_deref().map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or("info");
    match s {
        "error" => Ok(tracing::Level::ERROR),
        "warn" => Ok(tracing::Level::WARN),
        "info" => Ok(tracing::Level::INFO),
        "debug" => Ok(tracing::Level::DEBUG),
        "trace" => Ok(tracing::Level::TRACE),
        other => bail!("obs.otel.logs.level must be error|warn|info|debug|trace, got {other}"),
    }
}

fn batch_settings(sig: &OtelSignalOpts) -> Result<(usize, usize, Duration)> {
    let queue = sig.queue_size.unwrap_or(8192) as usize;
    let batch = sig.batch_size.unwrap_or(512) as usize;
    let delay = sig.delay_ms.unwrap_or(5000);
    if queue < 1 {
        bail!("queue_size must be >= 1");
    }
    if batch < 1 {
        bail!("batch_size must be >= 1");
    }
    if batch > queue {
        bail!("batch_size must be <= queue_size");
    }
    if delay < 10 {
        bail!("delay_ms must be >= 10");
    }
    Ok((queue, batch, Duration::from_millis(delay)))
}

fn apply_http<B: opentelemetry_otlp::WithExportConfig + opentelemetry_otlp::WithHttpConfig>(
    builder: B,
    endpoint: &str,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
) -> B {
    let mut b = builder
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_headers(headers.clone());
    if gzip {
        b = b.with_compression(opentelemetry_otlp::Compression::Gzip);
    }
    b
}

#[allow(clippy::too_many_arguments)]
fn build_tracer(
    resource: &opentelemetry_sdk::Resource,
    endpoint: &str,
    protocol: OtelProtocol,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
    sig: &OtelSignalOpts,
    ratio: f64,
) -> Result<SdkTracerProvider> {
    let (queue, batch, delay) = batch_settings(sig)?;
    let exporter = match protocol {
        OtelProtocol::HttpProtobuf => {
            let b = opentelemetry_otlp::SpanExporter::builder().with_http();
            apply_http(b, endpoint, gzip, headers, timeout)
                .build()
                .context("span exporter")?
        }
        OtelProtocol::Grpc => {
            #[cfg(feature = "grpc")]
            {
                grpc_span(endpoint, gzip, headers, timeout)?
            }
            #[cfg(not(feature = "grpc"))]
            {
                let _ = (endpoint, gzip, headers, timeout);
                bail!("rebuild with --features otel-grpc");
            }
        }
    };
    let cfg = opentelemetry_sdk::trace::BatchConfigBuilder::default()
        .with_max_queue_size(queue)
        .with_max_export_batch_size(batch)
        .with_scheduled_delay(delay)
        .build();
    let processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter)
        .with_batch_config(cfg)
        .build();
    Ok(SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
            opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(ratio),
        )))
        .with_span_processor(processor)
        .build())
}

#[allow(clippy::too_many_arguments)]
fn build_meter(
    resource: &opentelemetry_sdk::Resource,
    endpoint: &str,
    protocol: OtelProtocol,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
    interval: Duration,
) -> Result<SdkMeterProvider> {
    let exporter = match protocol {
        OtelProtocol::HttpProtobuf => {
            let b = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative);
            apply_http(b, endpoint, gzip, headers, timeout)
                .build()
                .context("metric exporter")?
        }
        OtelProtocol::Grpc => {
            #[cfg(feature = "grpc")]
            {
                grpc_metric(endpoint, gzip, headers, timeout)?
            }
            #[cfg(not(feature = "grpc"))]
            {
                let _ = (endpoint, gzip, headers, timeout);
                bail!("rebuild with --features otel-grpc");
            }
        }
    };
    let _ = timeout;
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();
    Ok(SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(reader)
        .build())
}

#[allow(clippy::too_many_arguments)]
fn build_logger(
    resource: &opentelemetry_sdk::Resource,
    endpoint: &str,
    protocol: OtelProtocol,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
    sig: &OtelSignalOpts,
) -> Result<SdkLoggerProvider> {
    let (queue, batch, delay) = batch_settings(sig)?;
    let exporter = match protocol {
        OtelProtocol::HttpProtobuf => {
            let b = opentelemetry_otlp::LogExporter::builder().with_http();
            apply_http(b, endpoint, gzip, headers, timeout)
                .build()
                .context("log exporter")?
        }
        OtelProtocol::Grpc => {
            #[cfg(feature = "grpc")]
            {
                grpc_log(endpoint, gzip, headers, timeout)?
            }
            #[cfg(not(feature = "grpc"))]
            {
                let _ = (endpoint, gzip, headers, timeout);
                bail!("rebuild with --features otel-grpc");
            }
        }
    };
    let cfg = opentelemetry_sdk::logs::BatchConfigBuilder::default()
        .with_max_queue_size(queue)
        .with_max_export_batch_size(batch)
        .with_scheduled_delay(delay)
        .build();
    let processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(exporter)
        .with_batch_config(cfg)
        .build();
    Ok(SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_log_processor(processor)
        .build())
}

#[cfg(feature = "grpc")]
fn grpc_span(
    endpoint: &str,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
) -> Result<opentelemetry_otlp::SpanExporter> {
    use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
    let mut meta = tonic::metadata::MetadataMap::new();
    for (k, v) in headers {
        if let (Ok(k), Ok(v)) = (
            tonic::metadata::MetadataKey::from_bytes(k.as_bytes()),
            v.parse(),
        ) {
            meta.insert(k, v);
        }
    }
    let mut b = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .with_metadata(meta);
    if gzip {
        b = b.with_compression(opentelemetry_otlp::Compression::Gzip);
    }
    b.build().context("grpc span exporter")
}

#[cfg(feature = "grpc")]
fn grpc_metric(
    endpoint: &str,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
) -> Result<opentelemetry_otlp::MetricExporter> {
    use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
    let mut meta = tonic::metadata::MetadataMap::new();
    for (k, v) in headers {
        if let (Ok(k), Ok(v)) = (
            tonic::metadata::MetadataKey::from_bytes(k.as_bytes()),
            v.parse(),
        ) {
            meta.insert(k, v);
        }
    }
    let mut b = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative)
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .with_metadata(meta);
    if gzip {
        b = b.with_compression(opentelemetry_otlp::Compression::Gzip);
    }
    b.build().context("grpc metric exporter")
}

#[cfg(feature = "grpc")]
fn grpc_log(
    endpoint: &str,
    gzip: bool,
    headers: &std::collections::HashMap<String, String>,
    timeout: Duration,
) -> Result<opentelemetry_otlp::LogExporter> {
    use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
    let mut meta = tonic::metadata::MetadataMap::new();
    for (k, v) in headers {
        if let (Ok(k), Ok(v)) = (
            tonic::metadata::MetadataKey::from_bytes(k.as_bytes()),
            v.parse(),
        ) {
            meta.insert(k, v);
        }
    }
    let mut b = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .with_metadata(meta);
    if gzip {
        b = b.with_compression(opentelemetry_otlp::Compression::Gzip);
    }
    b.build().context("grpc log exporter")
}

#[cfg(test)]
mod tests {
    use super::*;
    use nya_core::ObsOpts;

    #[test]
    fn empty_instance_rejected_when_enabled() {
        let obs = ObsOpts {
            otel: OtelOpts {
                enabled: true,
                endpoint: Some("http://127.0.0.1:4318".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let err = resolve_instance_name(&obs).unwrap_err().to_string();
        assert!(err.contains("instance_name"), "{err}");
    }

    #[test]
    fn batch_rejects_batch_gt_queue() {
        let sig = OtelSignalOpts {
            batch_size: Some(100),
            queue_size: Some(10),
            ..Default::default()
        };
        assert!(batch_settings(&sig).is_err());
    }

    #[test]
    fn batch_defaults_are_production() {
        let (q, b, d) = batch_settings(&OtelSignalOpts::default()).unwrap();
        assert_eq!(q, 8192);
        assert_eq!(b, 512);
        assert_eq!(d, Duration::from_millis(5000));
    }

    #[test]
    fn log_level_parse() {
        assert_eq!(
            parse_log_level(Some("debug")).unwrap(),
            tracing::Level::DEBUG
        );
        assert!(parse_log_level(Some("loud")).is_err());
    }

    #[test]
    fn resource_uses_same_instance_string() {
        let obs = ObsOpts {
            instance_name: Some("edge-sh-03".into()),
            ..Default::default()
        };
        let r = build_resource(
            "client",
            "0.1.0",
            "edge-sh-03",
            "20260831T000000Z-deadbeef",
            &obs,
        );
        let s = format!("{r:?}");
        assert!(s.contains("edge-sh-03"), "{s}");
        assert!(s.contains("20260831T000000Z-deadbeef"), "{s}");
        assert!(
            s.contains("nya-link-aggregation") || s.contains("nya.project"),
            "{s}"
        );
    }

    #[test]
    fn run_id_is_utc_prefix_plus_hex() {
        let id = resource::new_run_id();
        let (ts, suffix) = id.split_once('-').expect(&id);
        assert_eq!(ts.len(), 16, "{id}");
        assert!(ts.as_bytes()[8] == b'T' && ts.ends_with('Z'), "{id}");
        assert!(ts[..8].bytes().all(|c| c.is_ascii_digit()), "{id}");
        assert!(ts[9..15].bytes().all(|c| c.is_ascii_digit()), "{id}");
        assert_eq!(suffix.len(), 8, "{id}");
        assert!(suffix.bytes().all(|c| c.is_ascii_hexdigit()), "{id}");
        let again = resource::new_run_id();
        assert_ne!(id, again, "run id must differ across calls");
    }

    #[test]
    fn rustls_provider_then_blocking_http_client() {
        use std::io::{Read, Write};
        nya_core::install_crypto();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 256];
                let _ = s.read(&mut buf);
                let body = b"ok";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    std::str::from_utf8(body).unwrap()
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        let client = reqwest::blocking::Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .build()
            .expect("reqwest");
        let resp = client.get(format!("http://{addr}/")).send().expect("GET");
        assert!(resp.status().is_success(), "{}", resp.status());
    }

    #[test]
    fn sample_ratio_rejected_at_resolve() {
        let sig = 2.0_f64;
        assert!(!(0.0..=1.0).contains(&sig));
        let obs = ObsOpts {
            instance_name: Some("t".into()),
            otel: OtelOpts {
                enabled: true,
                endpoint: Some("http://127.0.0.1:1".into()),
                sample_ratio: Some(2.0),
                ..Default::default()
            },
            ..Default::default()
        };
        let err = install("client", "0.1.0", &obs).unwrap_err().to_string();
        assert!(err.contains("sample_ratio"), "{err}");
    }
}
