use std::time::SystemTime;

use anyhow::Result;
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::field::{Field, Visit};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

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
            .with_filter(EnvFilter::new(level.as_str().to_lowercase())),
        ),
        _ => None,
    };

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(traces_layer)
        .with(log_layer)
        .try_init()
        .ok();
    Ok(())
}

pub fn reset_filters() {}

fn fmt_filter(role: &str) -> EnvFilter {
    let crate_name = match role {
        "server" => "nya_server",
        _ => "nya_client",
    };
    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        format!("{crate_name}=info,nya_core=info")
            .parse()
            .expect("static filter")
    });
    let rust_log = std::env::var("RUST_LOG").unwrap_or_default();
    for target in [
        "opentelemetry",
        "opentelemetry_sdk",
        "opentelemetry_otlp",
        "tonic",
        "hyper",
        "reqwest",
    ] {
        if rust_log_has_target(&rust_log, target) {
            continue;
        }
        if let Ok(d) = format!("{target}=warn").parse() {
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
