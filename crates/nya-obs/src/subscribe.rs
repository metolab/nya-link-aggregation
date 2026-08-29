use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::field::{Field, Visit};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const EXPORT_ERROR_PULSE: Duration = Duration::from_secs(60);

pub fn init_fmt_only(role: &str) -> Result<()> {
    let filter = fmt_filter(role);
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init()
        .ok();
    Ok(())
}

pub fn init_otel(
    role: &str,
    tracer: Option<&SdkTracerProvider>,
    logger: Option<&SdkLoggerProvider>,
    traces_on: bool,
    log_level: Option<tracing::Level>,
    redact_targets: bool,
) -> Result<()> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(fmt_filter(role));

    let traces_layer = if traces_on {
        tracer.map(|tp| {
            tracing_opentelemetry::layer()
                .with_tracer(tp.tracer("nya"))
                .with_filter(EnvFilter::new("nya_otel=info"))
        })
    } else {
        None
    };

    let log_layer = match (logger, log_level) {
        (Some(lp), Some(level)) => Some(
            PiiLogLayer {
                provider: lp.clone(),
                redact_targets,
            }
            .with_filter(otel_log_filter(level)),
        ),
        _ => None,
    };

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(traces_layer)
        .with(log_layer)
        .with(ExportErrorPulse {
            state: Mutex::new(PulseState::default()),
        })
        .try_init()
        .ok();
    Ok(())
}

pub fn reset_filters() {}

fn fmt_filter(role: &str) -> EnvFilter {
    fmt_filter_from(role, std::env::var("RUST_LOG").ok().as_deref())
}

fn fmt_filter_from(role: &str, rust_log: Option<&str>) -> EnvFilter {
    let crate_name = match role {
        "server" => "nya_server",
        _ => "nya_client",
    };
    let rust_log = rust_log.unwrap_or("").trim();
    let mut filter = if rust_log.is_empty() {
        format!("{crate_name}=info,nya_core=info,nya_obs=info")
            .parse()
            .expect("static filter")
    } else {
        EnvFilter::try_new(rust_log).unwrap_or_else(|_| {
            format!("{crate_name}=info,nya_core=info,nya_obs=info")
                .parse()
                .expect("static filter")
        })
    };
    for target in [
        "opentelemetry",
        "opentelemetry_sdk",
        "opentelemetry_otlp",
        "tonic",
        "hyper",
        "reqwest",
    ] {
        if rust_log_has_target(rust_log, target) {
            continue;
        }
        if let Ok(d) = format!("{target}=off").parse() {
            filter = filter.add_directive(d);
        }
    }
    if !rust_log_has_target(rust_log, "nya_obs") {
        if let Ok(d) = "nya_obs=info".parse() {
            filter = filter.add_directive(d);
        }
    }
    filter
}

fn rust_log_has_target(rust_log: &str, target: &str) -> bool {
    rust_log.split(',').any(|d| {
        let d = d.trim();
        d == target || d.starts_with(&format!("{target}=")) || d.starts_with(&format!("{target}::"))
    })
}

fn otel_log_filter(level: tracing::Level) -> EnvFilter {
    let spec = format!(
        "{level},nya_core::obs=off,nya_otel=off,nya_obs=off,\
         opentelemetry=off,opentelemetry_sdk=off,opentelemetry_otlp=off,\
         hyper=off,reqwest=off,tonic=off",
        level = level.as_str().to_lowercase()
    );
    spec.parse().expect("static otel log filter")
}

fn is_otel_sdk_target(t: &str) -> bool {
    const PREFIXES: [&str; 3] = ["opentelemetry", "opentelemetry_sdk", "opentelemetry_otlp"];
    PREFIXES
        .iter()
        .any(|p| t == *p || t.starts_with(&format!("{p}::")))
}

#[derive(Default)]
struct PulseState {
    last_emit: Option<Instant>,
    suppressed: u64,
}

fn pulse_should_emit(st: &mut PulseState, now: Instant, every: Duration) -> Option<u64> {
    match st.last_emit {
        None => {
            st.last_emit = Some(now);
            st.suppressed = 0;
            Some(0)
        }
        Some(prev) if now.duration_since(prev) >= every => {
            let n = st.suppressed;
            st.last_emit = Some(now);
            st.suppressed = 0;
            Some(n)
        }
        Some(_) => {
            st.suppressed += 1;
            None
        }
    }
}

fn strip_url_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '?' {
            while matches!(chars.peek(), Some(ch) if *ch != '"' && !ch.is_whitespace()) {
                chars.next();
            }
            continue;
        }
        out.push(c);
    }
    out
}

fn parse_http_status(s: &str) -> Option<u16> {
    let rest = s.split("Status(").nth(1)?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

struct ExportErrorPulse {
    state: Mutex<PulseState>,
}

impl<S> Layer<S> for ExportErrorPulse
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::ERROR {
            return;
        }
        if !is_otel_sdk_target(event.metadata().target()) {
            return;
        }
        let mut vis = ErrVisit::default();
        event.record(&mut vis);
        let error = strip_url_query(vis.error.as_deref().unwrap_or("-"));
        let error = if error.len() > 256 {
            error.chars().take(256).collect()
        } else {
            error
        };
        let status = parse_http_status(&error).unwrap_or(0);
        let n = {
            let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
            pulse_should_emit(&mut g, Instant::now(), EXPORT_ERROR_PULSE)
        };
        if let Some(n) = n {
            tracing::error!(
                target: "nya_obs",
                sdk_target = event.metadata().target(),
                sdk_name = event.metadata().name(),
                status,
                error = error.as_str(),
                suppressed = n,
                "otlp export error"
            );
        }
    }
}

#[derive(Default)]
struct ErrVisit {
    error: Option<String>,
}

impl Visit for ErrVisit {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push(field.name(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field.name(), value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field.name(), value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field.name(), value.to_string());
    }
}

impl ErrVisit {
    fn push(&mut self, name: &str, value: String) {
        if matches!(name, "error" | "status" | "message") && self.error.is_none() {
            let v = value.trim_matches('"').to_string();
            if !v.is_empty() {
                self.error = Some(v);
            }
        }
    }
}

struct PiiLogLayer {
    provider: SdkLoggerProvider,
    redact_targets: bool,
}

impl<S> Layer<S> for PiiLogLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() == "nya_otel" {
            return;
        }
        use opentelemetry::logs::{LogRecord, Logger, LoggerProvider, Severity};
        let logger = self.provider.logger("nya");
        let mut rec = logger.create_log_record();
        let now = SystemTime::now();
        rec.set_timestamp(now);
        rec.set_observed_timestamp(now);
        rec.set_event_name(event.metadata().name());
        rec.set_target(event.metadata().target().to_string());
        rec.set_severity_number(match *event.metadata().level() {
            tracing::Level::ERROR => Severity::Error,
            tracing::Level::WARN => Severity::Warn,
            tracing::Level::INFO => Severity::Info,
            tracing::Level::DEBUG => Severity::Debug,
            tracing::Level::TRACE => Severity::Trace,
        });
        rec.set_severity_text(event.metadata().level().as_str());
        let sc = tracing::Span::current()
            .context()
            .span()
            .span_context()
            .clone();
        if sc.is_valid() {
            rec.set_trace_context(sc.trace_id(), sc.span_id(), Some(sc.trace_flags()));
        }
        let mut vis = AttrVisitor {
            redact: self.redact_targets,
            body: None,
            attrs: Vec::new(),
        };
        event.record(&mut vis);
        if let Some(body) = vis.body {
            rec.set_body(body.into());
        } else {
            rec.set_body(event.metadata().name().to_string().into());
        }
        for (k, v) in vis.attrs {
            rec.add_attribute(k, v);
        }
        rec.add_attribute("code.namespace", event.metadata().target().to_string());
        logger.emit(rec);
    }
}

struct AttrVisitor {
    redact: bool,
    body: Option<String>,
    attrs: Vec<(String, String)>,
}

fn denied(name: &str) -> bool {
    matches!(name, "psk" | "proof" | "exporter" | "session")
}

impl Visit for AttrVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push(field.name(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field.name(), value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field.name(), value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field.name(), value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field.name(), value.to_string());
    }
}

impl AttrVisitor {
    fn push(&mut self, name: &str, mut value: String) {
        if name == "message" {
            self.body = Some(value.trim_matches('"').to_string());
            return;
        }
        if denied(name) {
            return;
        }
        if self.redact
            && matches!(
                name,
                "host" | "target" | "nya.host" | "nya.target" | "server.address"
            )
        {
            value = "*".into();
        }
        self.attrs.push((name.to_string(), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_filter_injects_nya_obs_when_rust_log_omits_it() {
        let f = fmt_filter_from("server", Some("nya_server=info,nya_core=info"));
        let s = f.to_string();
        assert!(s.contains("nya_obs=info"), "{s}");
    }

    #[test]
    fn fmt_filter_respects_explicit_nya_obs() {
        let f = fmt_filter_from("server", Some("nya_obs=warn"));
        let s = f.to_string();
        assert!(s.contains("nya_obs=warn"), "{s}");
        assert!(!s.contains("nya_obs=info"), "{s}");
    }

    #[test]
    fn otel_log_filter_denies_snapshot_and_sdk() {
        let s = otel_log_filter(tracing::Level::INFO).to_string();
        assert!(s.contains("nya_core::obs=off"), "{s}");
        assert!(s.contains("opentelemetry_sdk=off"), "{s}");
        assert!(s.contains("nya_obs=off"), "{s}");
    }

    #[test]
    fn sdk_target_prefixes() {
        assert!(is_otel_sdk_target("opentelemetry"));
        assert!(is_otel_sdk_target("opentelemetry_sdk"));
        assert!(is_otel_sdk_target(
            "opentelemetry_sdk::logs::batch_log_processor"
        ));
        assert!(is_otel_sdk_target("opentelemetry_otlp"));
        assert!(!is_otel_sdk_target("nya_obs"));
        assert!(!is_otel_sdk_target("nya_server"));
    }

    #[test]
    fn pulse_first_then_suppress_then_emit() {
        let mut st = PulseState::default();
        let t0 = Instant::now();
        assert_eq!(pulse_should_emit(&mut st, t0, EXPORT_ERROR_PULSE), Some(0));
        for _ in 0..10 {
            assert_eq!(
                pulse_should_emit(&mut st, t0 + Duration::from_secs(10), EXPORT_ERROR_PULSE),
                None
            );
        }
        assert_eq!(st.suppressed, 10);
        assert_eq!(
            pulse_should_emit(&mut st, t0 + Duration::from_secs(60), EXPORT_ERROR_PULSE),
            Some(10)
        );
    }

    #[test]
    fn strip_query_keeps_path() {
        let s = strip_url_query(r#"Status(404) url="https://host/v1/logs?token=x""#);
        assert!(!s.contains("token=x"), "{s}");
        assert!(s.contains("https://host/v1/logs"), "{s}");
    }
}
