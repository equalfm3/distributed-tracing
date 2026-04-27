"""Service dependency graph extraction and analysis.

Aggregates parent-child span relationships across traces to build a
weighted directed graph of service dependencies.  Edge weights capture
call count, mean latency, and error rate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from ..trace.span import Span, StatusCode
from ..trace.trace import Trace, TraceNode


@dataclass
class EdgeStats:
    """Statistics for a directed edge (caller → callee) in the dependency graph.

    Attributes:
        caller: Calling service name.
        callee: Called service name.
        call_count: Number of calls observed.
        latencies_ms: Individual call latencies.
        error_count: Number of calls that resulted in errors.
    """

    caller: str
    callee: str
    call_count: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    error_count: int = 0

    @property
    def mean_latency_ms(self) -> float:
        """Mean call latency in milliseconds."""
        return float(np.mean(self.latencies_ms)) if self.latencies_ms else 0.0

    @property
    def p99_latency_ms(self) -> float:
        """99th percentile latency."""
        return float(np.percentile(self.latencies_ms, 99)) if self.latencies_ms else 0.0

    @property
    def error_rate(self) -> float:
        """Fraction of calls that resulted in errors."""
        return self.error_count / self.call_count if self.call_count > 0 else 0.0


@dataclass
class ServiceNode:
    """A service in the dependency graph.

    Attributes:
        name: Service name.
        span_count: Total spans from this service.
        error_count: Spans with error status.
        operations: Set of operation names seen.
    """

    name: str
    span_count: int = 0
    error_count: int = 0
    operations: set[str] = field(default_factory=set)


class DependencyGraph:
    """Directed graph of service dependencies extracted from traces.

    Attributes:
        nodes: Service nodes keyed by name.
        edges: Edge statistics keyed by (caller, callee).
    """

    def __init__(self) -> None:
        self.nodes: dict[str, ServiceNode] = {}
        self.edges: dict[tuple[str, str], EdgeStats] = {}

    def _ensure_node(self, name: str) -> ServiceNode:
        if name not in self.nodes:
            self.nodes[name] = ServiceNode(name=name)
        return self.nodes[name]

    def add_trace(self, trace: Trace) -> None:
        """Extract dependencies from a single trace.

        Args:
            trace: An assembled trace.
        """
        self._walk_node(trace.root)

    def _walk_node(self, node: TraceNode) -> None:
        """Recursively extract edges from the trace tree."""
        span = node.span
        parent_svc = self._ensure_node(span.service_name)
        parent_svc.span_count += 1
        parent_svc.operations.add(span.operation_name)
        if span.status == StatusCode.ERROR:
            parent_svc.error_count += 1

        for child in node.children:
            child_span = child.span
            if child_span.service_name != span.service_name:
                key = (span.service_name, child_span.service_name)
                if key not in self.edges:
                    self.edges[key] = EdgeStats(caller=key[0], callee=key[1])
                edge = self.edges[key]
                edge.call_count += 1
                if child_span.duration_ms > 0:
                    edge.latencies_ms.append(child_span.duration_ms)
                if child_span.status == StatusCode.ERROR:
                    edge.error_count += 1
            self._walk_node(child)

    def add_traces(self, traces: list[Trace]) -> None:
        """Extract dependencies from multiple traces.

        Args:
            traces: List of assembled traces.
        """
        for t in traces:
            self.add_trace(t)

    # -- queries ----------------------------------------------------------

    def upstream_of(self, service: str) -> list[str]:
        """Services that call the given service.

        Args:
            service: Target service name.

        Returns:
            List of caller service names.
        """
        return [caller for caller, callee in self.edges if callee == service]

    def downstream_of(self, service: str) -> list[str]:
        """Services called by the given service.

        Args:
            service: Source service name.

        Returns:
            List of callee service names.
        """
        return [callee for caller, callee in self.edges if caller == service]

    def detect_cycles(self) -> list[list[str]]:
        """Detect circular dependencies using DFS.

        Returns:
            List of cycles, each a list of service names.
        """
        adj: dict[str, list[str]] = {}
        for caller, callee in self.edges:
            adj.setdefault(caller, []).append(callee)

        cycles: list[list[str]] = []
        visited: set[str] = set()
        on_stack: set[str] = set()
        path: list[str] = []

        def _dfs(node: str) -> None:
            visited.add(node)
            on_stack.add(node)
            path.append(node)
            for neighbor in adj.get(node, []):
                if neighbor not in visited:
                    _dfs(neighbor)
                elif neighbor in on_stack:
                    idx = path.index(neighbor)
                    cycles.append(path[idx:] + [neighbor])
            path.pop()
            on_stack.discard(node)

        for node in list(adj.keys()):
            if node not in visited:
                _dfs(node)
        return cycles

    def summary(self) -> str:
        """Return a text summary of the dependency graph."""
        lines = [
            f"Dependency Graph: {len(self.nodes)} services, {len(self.edges)} edges",
            "",
        ]
        for (caller, callee), edge in sorted(self.edges.items()):
            err = f" err={edge.error_rate:.1%}" if edge.error_count else ""
            lines.append(
                f"  {caller} → {callee}: "
                f"{edge.call_count} calls, "
                f"mean={edge.mean_latency_ms:.1f}ms, "
                f"p99={edge.p99_latency_ms:.1f}ms{err}"
            )
        return "\n".join(lines)

    def to_adjacency_list(self) -> dict[str, list[dict[str, object]]]:
        """Export as an adjacency list for serialisation.

        Returns:
            Dict mapping caller to list of {callee, call_count, mean_latency_ms}.
        """
        adj: dict[str, list[dict[str, object]]] = {}
        for (caller, callee), edge in self.edges.items():
            adj.setdefault(caller, []).append({
                "callee": callee,
                "call_count": edge.call_count,
                "mean_latency_ms": round(edge.mean_latency_ms, 2),
                "error_rate": round(edge.error_rate, 4),
            })
        return adj


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from ..trace.trace import generate_trace, TraceAssembler

    parser = argparse.ArgumentParser(description="Service dependency graph demo")
    parser.add_argument("--traces", type=int, default=500, help="Number of traces")
    args = parser.parse_args()

    print("=== Service Dependency Graph Demo ===\n")

    assembler = TraceAssembler()
    for _ in range(args.traces):
        spans = generate_trace(n_services=6, max_depth=4)
        assembler.add_spans(spans)

    traces = assembler.assemble_all()
    print(f"Assembled {len(traces)} traces\n")

    graph = DependencyGraph()
    graph.add_traces(traces)
    print(graph.summary())

    # Show per-service details
    print("\nService details:")
    for name in sorted(graph.nodes.keys()):
        node = graph.nodes[name]
        up = graph.upstream_of(name)
        down = graph.downstream_of(name)
        print(f"  {name}: {node.span_count} spans, "
              f"{len(node.operations)} ops, "
              f"↑{len(up)} ↓{len(down)}")

    # Cycle detection
    cycles = graph.detect_cycles()
    if cycles:
        print(f"\nCircular dependencies detected ({len(cycles)}):")
        for cycle in cycles:
            print(f"  {' → '.join(cycle)}")
    else:
        print("\nNo circular dependencies detected.")
