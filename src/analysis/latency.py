"""Latency analysis: critical path detection, self-time, and percentiles.

Given assembled traces, this module computes where time is spent:
- **Self-time**: Time a span spends doing its own work (not waiting for children).
- **Critical path**: The longest chain of sequential operations — the minimum
  time the request could take even with infinite parallelism.
- **Percentile analysis**: p50, p90, p95, p99 latency distributions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from ..trace.span import Span
from ..trace.trace import Trace, TraceNode


@dataclass
class SpanTiming:
    """Timing breakdown for a single span.

    Attributes:
        span: The analysed span.
        self_time_ms: Time spent in this span's own work.
        child_time_ms: Time spent waiting for children.
        is_critical: Whether this span is on the critical path.
    """

    span: Span
    self_time_ms: float
    child_time_ms: float
    is_critical: bool = False


def compute_self_time(node: TraceNode) -> float:
    """Compute the self-time of a span (duration minus children's time).

    Handles overlapping children by computing the union of child intervals.

    Args:
        node: A trace tree node.

    Returns:
        Self-time in milliseconds.
    """
    if not node.span.end_time:
        return 0.0

    total_ms = node.span.duration_ms
    if not node.children:
        return total_ms

    # Merge overlapping child intervals
    intervals: list[tuple[float, float]] = []
    for child in node.children:
        if child.span.end_time:
            intervals.append((child.span.start_time, child.span.end_time))

    if not intervals:
        return total_ms

    intervals.sort()
    merged: list[tuple[float, float]] = [intervals[0]]
    for start, end in intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    child_time_s = sum(end - start for start, end in merged)
    self_time = total_ms - child_time_s * 1000.0
    return max(0.0, self_time)


def find_critical_path(node: TraceNode) -> list[SpanTiming]:
    """Find the critical path through a trace tree.

    The critical path is the root-to-leaf path with the maximum total
    self-time — the bottleneck chain that determines end-to-end latency.

    Args:
        node: Root of the trace tree.

    Returns:
        List of SpanTiming objects along the critical path.
    """
    timings = _compute_all_timings(node)

    # Find the path with maximum cumulative self-time
    best_path: list[SpanTiming] = []
    _find_critical(node, timings, [], best_path)

    for t in best_path:
        t.is_critical = True
    return best_path


def _compute_all_timings(node: TraceNode) -> dict[str, SpanTiming]:
    """Compute timings for every node in the tree.

    Args:
        node: Root node.

    Returns:
        Dict mapping span_id to SpanTiming.
    """
    result: dict[str, SpanTiming] = {}

    def _visit(n: TraceNode) -> None:
        self_t = compute_self_time(n)
        child_t = n.span.duration_ms - self_t
        result[n.span.span_id] = SpanTiming(
            span=n.span, self_time_ms=self_t, child_time_ms=child_t
        )
        for c in n.children:
            _visit(c)

    _visit(node)
    return result


def _find_critical(
    node: TraceNode,
    timings: dict[str, SpanTiming],
    current_path: list[SpanTiming],
    best_path: list[SpanTiming],
) -> None:
    """Recursive DFS to find the critical path."""
    timing = timings[node.span.span_id]
    current_path.append(timing)

    if not node.children:
        # Leaf — check if this path has the highest total self-time
        total = sum(t.self_time_ms for t in current_path)
        best_total = sum(t.self_time_ms for t in best_path) if best_path else -1.0
        if total > best_total:
            best_path.clear()
            best_path.extend(current_path)
    else:
        for child in node.children:
            _find_critical(child, timings, current_path, best_path)

    current_path.pop()


@dataclass
class LatencyReport:
    """Aggregate latency statistics across multiple traces.

    Attributes:
        durations_ms: Raw duration values.
        p50: 50th percentile (median).
        p90: 90th percentile.
        p95: 95th percentile.
        p99: 99th percentile.
        mean: Arithmetic mean.
        std: Standard deviation.
        min_ms: Minimum duration.
        max_ms: Maximum duration.
    """

    durations_ms: list[float] = field(default_factory=list)
    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    mean: float = 0.0
    std: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0

    @classmethod
    def from_traces(cls, traces: list[Trace]) -> LatencyReport:
        """Compute latency statistics from a list of traces.

        Args:
            traces: Assembled traces.

        Returns:
            A LatencyReport with percentile statistics.
        """
        durations = [t.duration_ms for t in traces if t.duration_ms > 0]
        if not durations:
            return cls()

        arr = np.array(durations)
        return cls(
            durations_ms=durations,
            p50=float(np.percentile(arr, 50)),
            p90=float(np.percentile(arr, 90)),
            p95=float(np.percentile(arr, 95)),
            p99=float(np.percentile(arr, 99)),
            mean=float(np.mean(arr)),
            std=float(np.std(arr)),
            min_ms=float(np.min(arr)),
            max_ms=float(np.max(arr)),
        )

    def summary(self) -> str:
        """Return a formatted summary string."""
        return (
            f"Latency (n={len(self.durations_ms)}): "
            f"p50={self.p50:.1f}ms p90={self.p90:.1f}ms "
            f"p95={self.p95:.1f}ms p99={self.p99:.1f}ms "
            f"mean={self.mean:.1f}ms std={self.std:.1f}ms "
            f"[{self.min_ms:.1f}ms, {self.max_ms:.1f}ms]"
        )


def per_service_latency(traces: list[Trace]) -> dict[str, LatencyReport]:
    """Compute latency statistics per service across traces.

    Args:
        traces: Assembled traces.

    Returns:
        Dict mapping service name to LatencyReport.
    """
    service_durations: dict[str, list[float]] = {}
    for trace in traces:
        for _, span in trace.root.walk():
            if span.duration_ms > 0:
                service_durations.setdefault(span.service_name, []).append(span.duration_ms)

    result: dict[str, LatencyReport] = {}
    for svc, durs in service_durations.items():
        arr = np.array(durs)
        result[svc] = LatencyReport(
            durations_ms=durs,
            p50=float(np.percentile(arr, 50)),
            p90=float(np.percentile(arr, 90)),
            p95=float(np.percentile(arr, 95)),
            p99=float(np.percentile(arr, 99)),
            mean=float(np.mean(arr)),
            std=float(np.std(arr)),
            min_ms=float(np.min(arr)),
            max_ms=float(np.max(arr)),
        )
    return result


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from ..trace.trace import generate_trace, TraceAssembler

    parser = argparse.ArgumentParser(description="Latency analysis demo")
    parser.add_argument("--traces", type=int, default=200, help="Number of traces")
    parser.add_argument("--show-critical-path", action="store_true", default=True)
    args = parser.parse_args()

    print("=== Latency Analysis Demo ===\n")

    assembler = TraceAssembler()
    for _ in range(args.traces):
        spans = generate_trace(n_services=5, max_depth=4)
        assembler.add_spans(spans)

    traces = assembler.assemble_all()
    print(f"Assembled {len(traces)} traces\n")

    # Overall latency report
    report = LatencyReport.from_traces(traces)
    print(report.summary())

    # Per-service latency
    print("\nPer-service latency:")
    svc_reports = per_service_latency(traces)
    for svc in sorted(svc_reports.keys()):
        r = svc_reports[svc]
        print(f"  {svc:25s} p50={r.p50:6.1f}ms  p99={r.p99:6.1f}ms  "
              f"n={len(r.durations_ms)}")

    # Critical path for first trace
    if args.show_critical_path and traces:
        trace = traces[0]
        print(f"\nCritical path for trace {trace.trace_id[:16]}…:")
        path = find_critical_path(trace.root)
        total_self = 0.0
        for i, timing in enumerate(path):
            marker = "→ " if timing.is_critical else "  "
            print(f"  {marker}[{timing.span.service_name}] "
                  f"{timing.span.operation_name} "
                  f"self={timing.self_time_ms:.1f}ms "
                  f"total={timing.span.duration_ms:.1f}ms")
            total_self += timing.self_time_ms
        print(f"  Critical path self-time: {total_self:.1f}ms "
              f"(trace total: {trace.duration_ms:.1f}ms)")
