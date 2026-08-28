use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use nya_core::{
    metric_descriptors, visit_metrics, HistSnap, InstrumentKind, MetricSink, ProcessSnapshot,
};
use opentelemetry::global;
use opentelemetry::KeyValue;

static REGISTERED: OnceLock<()> = OnceLock::new();
static SOURCE: Mutex<Option<Arc<dyn Fn() -> ProcessSnapshot + Send + Sync>>> = Mutex::new(None);
static CACHE: Mutex<Option<(Instant, ProcessSnapshot)>> = Mutex::new(None);

pub fn register(src: Arc<dyn Fn() -> ProcessSnapshot + Send + Sync>) {
    *SOURCE.lock().unwrap_or_else(|e| e.into_inner()) = Some(src);
    if REGISTERED.set(()).is_err() {
        return;
    }
    let meter = global::meter("nya");
    for desc in metric_descriptors() {
        match desc.kind {
            InstrumentKind::Counter => {
                let name = desc.name;
                let _ = meter
                    .u64_observable_counter(name)
                    .with_description(desc.help)
                    .with_callback(move |obs| {
                        let snap = scrape();
                        let mut s = PickCounter { name, value: 0 };
                        visit_metrics(&snap, &mut s);
                        obs.observe(s.value, &[]);
                    })
                    .build();
            }
            InstrumentKind::Gauge => {
                let name = desc.name;
                let _ = meter
                    .u64_observable_gauge(name)
                    .with_description(desc.help)
                    .with_callback(move |obs| {
                        let snap = scrape();
                        let mut s = PickGauge {
                            name,
                            points: Vec::new(),
                        };
                        visit_metrics(&snap, &mut s);
                        for (labels, v) in s.points {
                            let kvs: Vec<KeyValue> = labels
                                .into_iter()
                                .map(|(k, val)| KeyValue::new(k, val))
                                .collect();
                            obs.observe(v, &kvs);
                        }
                    })
                    .build();
            }
            InstrumentKind::Histogram => {
                let name = desc.name;
                let help = desc.help;
                let bucket = format!("{name}_bucket");
                let sum = format!("{name}_sum");
                let count = format!("{name}_count");
                let _ = meter
                    .u64_observable_counter(bucket)
                    .with_description(help)
                    .with_callback(move |obs| {
                        let snap = scrape();
                        let mut s = PickHist {
                            name,
                            snap: None,
                            bounds: &[],
                        };
                        visit_metrics(&snap, &mut s);
                        if let (Some(h), bounds) = (s.snap, s.bounds) {
                            let mut cum = 0u64;
                            for (i, &le) in bounds.iter().enumerate() {
                                cum += h.buckets.get(i).copied().unwrap_or(0);
                                obs.observe(cum, &[KeyValue::new("le", le.to_string())]);
                            }
                            if h.buckets.len() > bounds.len() {
                                cum += h.buckets[bounds.len()];
                            }
                            obs.observe(cum, &[KeyValue::new("le", "+Inf")]);
                        }
                    })
                    .build();
                let sname = name;
                let _ = meter
                    .u64_observable_counter(sum)
                    .with_description(help)
                    .with_callback(move |obs| {
                        let snap = scrape();
                        let mut s = PickHist {
                            name: sname,
                            snap: None,
                            bounds: &[],
                        };
                        visit_metrics(&snap, &mut s);
                        if let Some(h) = s.snap {
                            obs.observe(h.sum, &[]);
                        }
                    })
                    .build();
                let cname = name;
                let _ = meter
                    .u64_observable_counter(count)
                    .with_description(help)
                    .with_callback(move |obs| {
                        let snap = scrape();
                        let mut s = PickHist {
                            name: cname,
                            snap: None,
                            bounds: &[],
                        };
                        visit_metrics(&snap, &mut s);
                        if let Some(h) = s.snap {
                            obs.observe(h.count, &[]);
                        }
                    })
                    .build();
            }
        }
    }
}

fn scrape() -> ProcessSnapshot {
    const TTL: Duration = Duration::from_millis(50);
    {
        let g = CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, snap)) = g.as_ref() {
            if at.elapsed() < TTL {
                return snap.clone();
            }
        }
    }
    let src = SOURCE.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let snap = src.map(|f| f()).unwrap_or_default();
    *CACHE.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), snap.clone()));
    snap
}

struct PickCounter {
    name: &'static str,
    value: u64,
}

impl MetricSink for PickCounter {
    fn counter(&mut self, name: &'static str, _help: &'static str, value: u64) {
        if name == self.name {
            self.value = value;
        }
    }
    fn gauge(&mut self, _n: &'static str, _h: &'static str, _l: &[(&'static str, &str)], _v: u64) {}
    fn histogram(&mut self, _n: &'static str, _h: &'static str, _b: &'static [u64], _s: &HistSnap) {
    }
}

struct PickGauge {
    name: &'static str,
    points: Vec<(Vec<(String, String)>, u64)>,
}

impl MetricSink for PickGauge {
    fn counter(&mut self, _n: &'static str, _h: &'static str, _v: u64) {}
    fn gauge(
        &mut self,
        name: &'static str,
        _help: &'static str,
        labels: &[(&'static str, &str)],
        value: u64,
    ) {
        if name == self.name {
            self.points.push((
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
                value,
            ));
        }
    }
    fn histogram(&mut self, _n: &'static str, _h: &'static str, _b: &'static [u64], _s: &HistSnap) {
    }
}

struct PickHist {
    name: &'static str,
    snap: Option<HistSnap>,
    bounds: &'static [u64],
}

impl MetricSink for PickHist {
    fn counter(&mut self, _n: &'static str, _h: &'static str, _v: u64) {}
    fn gauge(&mut self, _n: &'static str, _h: &'static str, _l: &[(&'static str, &str)], _v: u64) {}
    fn histogram(
        &mut self,
        name: &'static str,
        _help: &'static str,
        bounds: &'static [u64],
        snap: &HistSnap,
    ) {
        if name == self.name {
            self.snap = Some(snap.clone());
            self.bounds = bounds;
        }
    }
}
