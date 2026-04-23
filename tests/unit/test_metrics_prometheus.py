from greencompute_persistence.metrics import MetricsStore, render_prometheus_text


def test_render_prometheus_text_includes_counters_gauges_and_summaries() -> None:
    metrics = MetricsStore()
    metrics.increment("build.published")
    metrics.set_gauge("workers.builder.running", 1.0)
    metrics.observe("invoke.latency_ms", 12.5)
    metrics.observe("invoke.latency_ms", 7.5)

    payload = render_prometheus_text("greencompute-builder", metrics)

    assert 'greencompute_service_info{service="greencompute-builder"} 1' in payload
    assert 'greencompute_build_published{service="greencompute-builder"} 1.0' in payload
    assert 'greencompute_workers_builder_running{service="greencompute-builder"} 1.0' in payload
    assert 'greencompute_invoke_latency_ms_count{service="greencompute-builder"} 2.0' in payload
    assert 'greencompute_invoke_latency_ms_sum{service="greencompute-builder"} 20.0' in payload
