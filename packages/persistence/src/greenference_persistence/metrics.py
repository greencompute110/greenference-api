from __future__ import annotations

from collections import defaultdict
from threading import Lock


class MetricsStore:
    def __init__(self) -> None:
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._summaries: dict[str, dict[str, float]] = defaultdict(
            lambda: {"count": 0.0, "sum": 0.0, "max": 0.0}
        )
        self._lock = Lock()

    def increment(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            self._counters[name] += value

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            summary = self._summaries[name]
            summary["count"] += 1.0
            summary["sum"] += value
            summary["max"] = max(summary["max"], value)

    def snapshot(self) -> dict[str, dict[str, float]]:
        with self._lock:
            return {
                "counters": dict(sorted(self._counters.items())),
                "gauges": dict(sorted(self._gauges.items())),
                "summaries": {
                    key: dict(values) for key, values in sorted(self._summaries.items())
                },
            }


_stores: dict[str, MetricsStore] = {}


def get_metrics_store(service_name: str) -> MetricsStore:
    store = _stores.get(service_name)
    if store is None:
        store = MetricsStore()
        _stores[service_name] = store
    return store


def render_prometheus_text(service_name: str, metrics: MetricsStore) -> str:
    snapshot = metrics.snapshot()
    lines = [
        "# HELP greenference_service_info Static service information",
        "# TYPE greenference_service_info gauge",
        f'greenference_service_info{{service="{service_name}"}} 1',
    ]
    for name, value in snapshot["counters"].items():
        metric = _metric_name(name)
        lines.append(f"# TYPE {metric} counter")
        lines.append(f'{metric}{{service="{service_name}"}} {value}')
    for name, value in snapshot["gauges"].items():
        metric = _metric_name(name)
        lines.append(f"# TYPE {metric} gauge")
        lines.append(f'{metric}{{service="{service_name}"}} {value}')
    for name, summary in snapshot["summaries"].items():
        metric = _metric_name(name)
        lines.append(f"# TYPE {metric} summary")
        lines.append(f'{metric}_count{{service="{service_name}"}} {summary["count"]}')
        lines.append(f'{metric}_sum{{service="{service_name}"}} {summary["sum"]}')
        lines.append(f'{metric}_max{{service="{service_name}"}} {summary["max"]}')
    return "\n".join(lines) + "\n"


def _metric_name(name: str) -> str:
    sanitized = name.replace(".", "_").replace("-", "_")
    return f"greenference_{sanitized}"
