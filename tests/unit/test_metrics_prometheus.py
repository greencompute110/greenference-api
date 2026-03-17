from greenference_persistence.metrics import MetricsStore, render_prometheus_text


def test_render_prometheus_text_includes_counters_gauges_and_summaries() -> None:
    metrics = MetricsStore()
    metrics.increment("build.published")
    metrics.set_gauge("workers.builder.running", 1.0)
    metrics.observe("invoke.latency_ms", 12.5)
    metrics.observe("invoke.latency_ms", 7.5)

    payload = render_prometheus_text("greenference-builder", metrics)

    assert 'greenference_service_info{service="greenference-builder"} 1' in payload
    assert 'greenference_build_published{service="greenference-builder"} 1.0' in payload
    assert 'greenference_workers_builder_running{service="greenference-builder"} 1.0' in payload
    assert 'greenference_invoke_latency_ms_count{service="greenference-builder"} 2.0' in payload
    assert 'greenference_invoke_latency_ms_sum{service="greenference-builder"} 20.0' in payload
