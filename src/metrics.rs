use prometheus::{
    Registry, HistogramVec, IntCounterVec, IntGauge, Gauge,
    TextEncoder, Encoder, Opts, HistogramOpts,
};
use std::sync::Arc;

pub struct MetricsRegistry
{
    pub registry: Registry,
    pub requests_total: IntCounterVec,
    pub request_duration: HistogramVec,
    pub evictions_total: IntCounterVec,
    pub memtable_bytes: IntGauge,
    pub average_retention_score: Gauge,
}

impl MetricsRegistry
{
    // initialize all prometheus metrics and register with the global registry
    pub fn new() -> Self
    {
        let registry = Registry::new();

        let request_opts = Opts::new("ebbinghaus_requests_total", "total requests processed");
        let requests_total = IntCounterVec::new(request_opts, &["op", "status"]).unwrap();

        let duration_opts = HistogramOpts::new("ebbinghaus_request_duration_seconds", "request latency in seconds")
            // microsecond-level granular buckets for tail latency measurement (0.1ms to 10ms)
            .buckets(vec![0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.010]);
        let request_duration = HistogramVec::new(duration_opts, &["op"]).unwrap();

        let evict_opts = Opts::new("ebbinghaus_evictions_total", "total records evicted by decay");
        let evictions_total = IntCounterVec::new(evict_opts, &["reason"]).unwrap();

        let memtable_bytes = IntGauge::new("ebbinghaus_memtable_bytes", "current working memory size in bytes").unwrap();
        let average_retention_score = Gauge::new("ebbinghaus_average_retention_score", "aggregate decay score of active data").unwrap();

        registry.register(Box::new(requests_total.clone())).unwrap();
        registry.register(Box::new(request_duration.clone())).unwrap();
        registry.register(Box::new(evictions_total.clone())).unwrap();
        registry.register(Box::new(memtable_bytes.clone())).unwrap();
        registry.register(Box::new(average_retention_score.clone())).unwrap();

        MetricsRegistry
        {
            registry,
            requests_total,
            request_duration,
            evictions_total,
            memtable_bytes,
            average_retention_score,
        }
    }

    // render prometheus exposition text format
    pub fn render(&self) -> String
    {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap_or_default()
    }
}

// lightweight http server exposing the /metrics endpoint
pub async fn start_metrics_server(
    listener: tokio::net::TcpListener,
    metrics: Arc<MetricsRegistry>,
) -> std::io::Result<()>
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop
    {
        let (mut socket, _) = listener.accept().await?;
        let metrics = metrics.clone();

        tokio::spawn(async move
        {
            let mut buf = [0u8; 1024];
            let n = socket.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);

            if req.starts_with("GET /metrics")
            {
                let body = metrics.render();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
            else
            {
                let not_found = "HTTP/1.1 404 NOT FOUND\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(not_found.as_bytes()).await;
            }
        });
    }
}
