use nya_core::ObsOpts;
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::Resource;

use crate::NAMESPACE;

pub fn build_resource(role: &str, version: &str, instance: &str, obs: &ObsOpts) -> Resource {
    let service_name = match role {
        "server" => "nya-server",
        _ => "nya-client",
    };
    let mut attrs = vec![
        KeyValue::new("service.namespace", NAMESPACE),
        KeyValue::new("service.version", version.to_string()),
        KeyValue::new("service.instance.id", instance.to_string()),
        KeyValue::new("nya.project", NAMESPACE),
        KeyValue::new("nya.role", role.to_string()),
        KeyValue::new("nya.instance.name", instance.to_string()),
        KeyValue::new("process.pid", Value::I64(i64::from(std::process::id()))),
        KeyValue::new("host.name", host_name()),
    ];
    if let Some(env) = obs
        .otel
        .environment
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        attrs.push(KeyValue::new("deployment.environment", env.to_string()));
    }
    if let Ok(extra) = std::env::var("OTEL_RESOURCE_ATTRIBUTES") {
        let reserved = [
            "service.namespace",
            "service.name",
            "service.instance.id",
            "nya.project",
            "nya.instance.name",
        ];
        for part in extra.split(',') {
            if let Some((k, v)) = part.split_once('=') {
                let k = k.trim();
                if reserved.contains(&k) {
                    continue;
                }
                attrs.push(KeyValue::new(k.to_string(), v.trim().to_string()));
            }
        }
    }
    Resource::builder()
        .with_service_name(service_name)
        .with_attributes(attrs)
        .build()
}

pub fn host_name() -> String {
    if let Ok(h) = std::env::var("HOSTNAME") {
        let t = h.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes a C string into `buf`.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc == 0 {
        let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
        if let Ok(s) = std::str::from_utf8(&buf[..len]) {
            let t = s.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }
    tracing::warn!("hostname unavailable; host.name=unknown");
    "unknown".into()
}
