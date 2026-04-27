"""Anomaly detection: latency outliers and error pattern detection.

Identifies traces and spans that deviate significantly from normal
behaviour — latency spikes, unusual error patterns, and services
exhibiting degraded performance.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np

from ..trace.span import Span, StatusCode
from ..trace.trace import Trace, TraceNode


class AnomalyType(Enum):
    """Classification of detected anomalies."""

    LATENCY_OUTLIER = "latency_outlier"
    ERROR_SPIKE = "error_spike"
    SLOW_SERVICE = "slow_service"
    HIGH_FAN_OUT = "high_fan_out"


@dataclass
class Anomaly:
    """A detected anomaly in trace data.

    Attributes:
        anomaly_type: Classification of the anomaly.
        trace_id: The trace where the anomaly was found.
        span_id: The specific span (if applicable).
        service_name: The affected service.
        description: Human-readable description.
        severity: Score from 0.0 (minor) to 1.0 (critical).
        value: The anomalous metric value.
        threshold: The threshold that was exceeded.
    """

    anomaly_type: AnomalyType
    trace_id: str
    span_id: Optional[str]
    service_name: str
    description: str
    severity: float
    value: float
    threshold: float


@dataclass
class AnomalyDetector:
    """Detects anomalies in trace data using statistical methods.

    Uses z-score based outlier detection for latency and threshold-based
    detection for error rates and fan-out.

    Attributes:
        z_threshold: Z-score threshold for latency outliers (default 2.5).
        error_rate_threshold: Error rate above which a service is flagged.
        fan_out_threshold: Max children before flagging high fan-out.
    """

    z_threshold: float = 2.5
    error_rate_threshold: float = 0.1
    fan_out_threshold: int = 10

    def detect(self, traces: list[Trace]) -> list[Anomaly]:
        """Run all anomaly detectors on a set of traces.

        Args:
            traces: Assembled traces to analyse.

        Returns:
            List of detected anomalies, sorted by severity (descending).
        """
        anomalies: list[Anomaly] = []
        anomalies.extend(self._detect_latency_outliers(traces))
        anomalies.extend(self._detect_error_spikes(traces))
        anomalies.extend(self._detect_slow_services(traces))
        anomalies.extend(self._detect_high_fan_out(traces))
        anomalies.sort(key=lambda a: a.severity, reverse=True)
        return anomalies

    def _detect_latency_outliers(self, traces: list[Trace]) -> list[Anomaly]:
        """Find traces with abnormally high latency."""
        durations = [t.duration_ms for t in traces if t.duration_ms > 0]
        if len(durations) < 5:
            return []

        arr = np.array(durations)
        mean = float(np.mean(arr))
        std = float(np.std(arr))
        if std < 1e-9:
            return []

        anomalies: list[Anomaly] = []
        for trace in traces:
            if trace.duration_ms <= 0:
                continue
            z = (trace.duration_ms - mean) / std
            if z > self.z_threshold:
                severity = min(1.0, z / (self.z_threshold * 2))
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.LATENCY_OUTLIER,
                    trace_id=trace.trace_id,
                    span_id=trace.root.span.span_id,
                    service_name=trace.root.span.service_name,
                    description=(
                        f"Trace latency {trace.duration_ms:.1f}ms is "
                        f"{z:.1f}σ above mean ({mean:.1f}ms)"
                    ),
                    severity=severity,
                    value=trace.duration_ms,
                    threshold=mean + self.z_threshold * std,
                ))
        return anomalies

    def _detect_error_spikes(self, traces: list[Trace]) -> list[Anomaly]:
        """Find services with high error rates."""
        service_counts: dict[str, int] = {}
        service_errors: dict[str, int] = {}

        for trace in traces:
            for _, span in trace.root.walk():
                svc = span.service_name
                service_counts[svc] = service_counts.get(svc, 0) + 1
                if span.status == StatusCode.ERROR:
                    service_errors[svc] = service_errors.get(svc, 0) + 1

        anomalies: list[Anomaly] = []
        for svc, total in service_counts.items():
            errors = service_errors.get(svc, 0)
            rate = errors / total
            if rate > self.error_rate_threshold and total >= 5:
                severity = min(1.0, rate / 0.5)
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.ERROR_SPIKE,
                    trace_id="aggregate",
                    span_id=None,
                    service_name=svc,
                    description=(
                        f"Service {svc} has {rate:.1%} error rate "
                        f"({errors}/{total} spans)"
                    ),
                    severity=severity,
                    value=rate,
                    threshold=self.error_rate_threshold,
                ))
        return anomalies

    def _detect_slow_services(self, traces: list[Trace]) -> list[Anomaly]:
        """Find services whose mean latency is an outlier among all services."""
        service_latencies: dict[str, list[float]] = {}
        for trace in traces:
            for _, span in trace.root.walk():
                if span.duration_ms > 0:
                    service_latencies.setdefault(span.service_name, []).append(
                        span.duration_ms
                    )

        if len(service_latencies) < 3:
            return []

        means = {svc: float(np.mean(lats)) for svc, lats in service_latencies.items()}
        all_means = np.array(list(means.values()))
        global_mean = float(np.mean(all_means))
        global_std = float(np.std(all_means))

        if global_std < 1e-9:
            return []

        anomalies: list[Anomaly] = []
        for svc, svc_mean in means.items():
            z = (svc_mean - global_mean) / global_std
            if z > self.z_threshold:
                severity = min(1.0, z / (self.z_threshold * 2))
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.SLOW_SERVICE,
                    trace_id="aggregate",
                    span_id=None,
                    service_name=svc,
                    description=(
                        f"Service {svc} mean latency {svc_mean:.1f}ms is "
                        f"{z:.1f}σ above average ({global_mean:.1f}ms)"
                    ),
                    severity=severity,
                    value=svc_mean,
                    threshold=global_mean + self.z_threshold * global_std,
                ))
        return anomalies

    def _detect_high_fan_out(self, traces: list[Trace]) -> list[Anomaly]:
        """Find spans with unusually many children (high fan-out)."""
        anomalies: list[Anomaly] = []

        def _check(node: TraceNode) -> None:
            n_children = len(node.children)
            if n_children > self.fan_out_threshold:
                severity = min(1.0, n_children / (self.fan_out_threshold * 3))
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.HIGH_FAN_OUT,
                    trace_id=node.span.trace_id,
                    span_id=node.span.span_id,
                    service_name=node.span.service_name,
                    description=(
                        f"Span {node.span.operation_name} in "
                        f"{node.span.service_name} has {n_children} children "
                        f"(threshold: {self.fan_out_threshold})"
                    ),
                    severity=severity,
                    value=float(n_children),
                    threshold=float(self.fan_out_threshold),
                ))
            for child in node.children:
                _check(child)

        for trace in traces:
            _check(trace.root)
        return anomalies


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from ..trace.trace import generate_trace, TraceAssembler

    parser = argparse.ArgumentParser(description="Anomaly detection demo")
    parser.add_argument("--traces", type=int, default=300, help="Number of traces")
    args = parser.parse_args()

    print("=== Anomaly Detection Demo ===\n")

    assembler = TraceAssembler()
    for _ in range(args.traces):
        spans = generate_trace(n_services=5, max_depth=4)
        assembler.add_spans(spans)

    traces = assembler.assemble_all()
    print(f"Analysed {len(traces)} traces\n")

    detector = AnomalyDetector(
        z_threshold=2.0,
        error_rate_threshold=0.05,
        fan_out_threshold=5,
    )
    anomalies = detector.detect(traces)

    if anomalies:
        print(f"Found {len(anomalies)} anomalies:\n")
        for a in anomalies[:15]:
            icon = {
                AnomalyType.LATENCY_OUTLIER: "⏱",
                AnomalyType.ERROR_SPIKE: "❌",
                AnomalyType.SLOW_SERVICE: "🐢",
                AnomalyType.HIGH_FAN_OUT: "🔀",
            }.get(a.anomaly_type, "?")
            print(f"  {icon} [{a.severity:.2f}] {a.description}")
    else:
        print("No anomalies detected.")
