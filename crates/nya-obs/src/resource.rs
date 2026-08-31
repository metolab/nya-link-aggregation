use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use nya_core::ObsOpts;
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::Resource;

use crate::NAMESPACE;

pub fn build_resource(
    role: &str,
    version: &str,
    instance: &str,
    run_id: &str,
    obs: &ObsOpts,
) -> Resource {
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
        KeyValue::new("nya.instance.run_id", run_id.to_string()),
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
            "nya.instance.run_id",
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

/// Per-process run id: `YYYYMMDDTHHMMSSZ` + 8 hex. Not an RFC 4122 UUID.
pub(crate) fn new_run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}-{}", utc_compact(now.as_secs()), random_hex8())
}

fn utc_compact(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let tod = unix_secs % 86_400;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    // Howard Hinnant civil-from-days (proleptic Gregorian, Unix epoch).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    format!("{y:04}{m:02}{d:02}T{hour:02}{min:02}{sec:02}Z")
}

fn random_hex8() -> String {
    let mut buf = [0u8; 4];
    let filled = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .is_ok();
    if !filled {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mixed = nanos ^ u128::from(std::process::id()).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        buf = (mixed as u32).to_le_bytes();
    }
    format!("{:08x}", u32::from_le_bytes(buf))
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

#[cfg(test)]
mod tests {
    use super::utc_compact;

    #[test]
    fn utc_compact_unix_epoch() {
        assert_eq!(utc_compact(0), "19700101T000000Z");
    }

    #[test]
    fn utc_compact_known_instants() {
        assert_eq!(utc_compact(1_700_000_000), "20231114T221320Z");
        assert_eq!(utc_compact(1_582_934_400), "20200229T000000Z");
        assert_eq!(utc_compact(1_767_225_600), "20260101T000000Z");
    }
}
