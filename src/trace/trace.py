"""Trace assembly: reconstruct trace trees from unordered span collections.

Spans arrive at the collector in arbitrary order — a child span may arrive
before its parent.  The assembler buffers spans by trace ID and reconstructs
the tree once all spans have arrived (or a timeout expires).  Orphan spans
(whose parent never arrives) are attached to a synthetic root.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Optional

from .span import Span, SpanKind, StatusCode
from .context import generate_trace_id, generate_span_id


@dataclass
class TraceNode:
    """A node in the assembled trace tree.

    Attributes:
        span: The span at this node.
        children: Ordered child nodes (by start time).
    """

    span: Span
    children: list[TraceNode] = field(default_factory=list)

    @property
    def depth(self) -> int:
        """Maximum depth of the sub-tree rooted at this node."""
        if not self.children:
            return 0
        return 1 + max(c.depth for c in self.children)

    def walk(self) -> list[tuple[int, Span]]:
        """Depth-first walk yielding ``(depth, span)`` pairs.

        Returns:
            List of (depth, span) tuples in pre-order.
        """
        result: list[tuple[int, Span]] = []
        self._walk(0, result)
        return result

    def _walk(self, depth: int, acc: list[tuple[int, Span]]) -> None:
        acc.append((depth, self.span))
        for child in self.children:
            child._walk(depth + 1, acc)


@dataclass
class Trace:
    """An assembled trace — a tree of spans sharing a trace ID.

    Attributes:
        trace_id: The shared trace identifier.
        root: The root node of the trace tree.
        span_count: Total number of spans in the trace.
    """

    trace_id: str
    root: TraceNode
    span_count: int

    @property
    def duration_ms(self) -> float:
        """Total trace duration from root span."""
        return self.root.span.duration_ms

    @property
    def service_names(self) -> set[str]:
        """Set of all service names in the trace."""
        return {span.service_name for _, span in self.root.walk()}

    def print_tree(self, indent: int = 2) -> str:
        """Pretty-print the trace tree.

        Args:
            indent: Number of spaces per depth level.

        Returns:
            A multi-line string representation.
        """
        lines: list[str] = []
        for depth, span in self.root.walk():
            prefix = " " * (depth * indent)
            dur = f"{span.duration_ms:.1f}ms" if span.end_time else "active"
            lines.append(f"{prefix}[{span.service_name}] {span.operation_name} ({dur})")
        return "\n".join(lines)


class TraceAssembler:
    """Assembles complete traces from unordered span streams.

    Spans are buffered by trace ID.  When ``assemble`` is called, the
    assembler reconstructs the tree for each trace ID, handling orphan
    spans by attaching them to a synthetic root.

    Attributes:
        _buffers: Mapping from trace_id to list of buffered spans.
    """

    def __init__(self) -> None:
        self._buffers: dict[str, list[Span]] = {}

    def add_span(self, span: Span) -> None:
        """Buffer a span for later assembly.

        Args:
            span: The span to buffer.
        """
        self._buffers.setdefault(span.trace_id, []).append(span)

    def add_spans(self, spans: list[Span]) -> None:
        """Buffer multiple spans.

        Args:
            spans: Spans to buffer.
        """
        for s in spans:
            self.add_span(s)

    @property
    def trace_ids(self) -> list[str]:
        """Return all buffered trace IDs."""
        return list(self._buffers.keys())

    def assemble(self, trace_id: str) -> Optional[Trace]:
        """Assemble a trace tree from buffered spans.

        Args:
            trace_id: The trace to assemble.

        Returns:
            An assembled Trace, or None if no spans exist for the ID.
        """
        spans = self._buffers.get(trace_id)
        if not spans:
            return None

        span_map: dict[str, Span] = {s.span_id: s for s in spans}
        node_map: dict[str, TraceNode] = {sid: TraceNode(span=s) for sid, s in span_map.items()}

        root_nodes: list[TraceNode] = []
        for span in spans:
            node = node_map[span.span_id]
            if span.parent_span_id and span.parent_span_id in node_map:
                node_map[span.parent_span_id].children.append(node)
            else:
                root_nodes.append(node)

        # Sort children by start time at every level
        for node in node_map.values():
            node.children.sort(key=lambda n: n.span.start_time)

        if len(root_nodes) == 1:
            root = root_nodes[0]
        else:
            # Multiple roots → create synthetic root
            root_nodes.sort(key=lambda n: n.span.start_time)
            synthetic = Span(
                trace_id=trace_id,
                span_id=generate_span_id(),
                parent_span_id=None,
                operation_name="synthetic-root",
                service_name="assembler",
                start_time=min(s.start_time for s in spans),
                end_time=max((s.end_time or s.start_time) for s in spans),
            )
            root = TraceNode(span=synthetic, children=root_nodes)

        return Trace(trace_id=trace_id, root=root, span_count=len(spans))

    def assemble_all(self) -> list[Trace]:
        """Assemble all buffered traces.

        Returns:
            List of assembled Trace objects.
        """
        traces: list[Trace] = []
        for tid in list(self._buffers.keys()):
            t = self.assemble(tid)
            if t:
                traces.append(t)
        return traces


# -----------------------------------------------------------------------
# Trace generator — creates realistic synthetic traces
# -----------------------------------------------------------------------

_SERVICES = [
    ("api-gateway", ["GET /api/users", "GET /api/orders", "POST /api/checkout"]),
    ("auth-service", ["verify_token", "refresh_token"]),
    ("user-service", ["get_user", "list_users", "update_user"]),
    ("order-service", ["create_order", "get_order", "list_orders"]),
    ("payment-service", ["charge", "refund", "validate_card"]),
    ("inventory-service", ["check_stock", "reserve_item"]),
    ("notification-service", ["send_email", "send_sms"]),
    ("cache-service", ["cache_get", "cache_set"]),
    ("db-service", ["SELECT", "INSERT", "UPDATE"]),
]


def generate_trace(
    n_services: int = 5,
    max_depth: int = 4,
    base_latency_ms: float = 5.0,
) -> list[Span]:
    """Generate a realistic synthetic trace.

    Args:
        n_services: Number of distinct services in the trace.
        max_depth: Maximum depth of the span tree.
        base_latency_ms: Base latency per span in milliseconds.

    Returns:
        A list of spans forming a single trace (unordered).
    """
    services = random.sample(_SERVICES, min(n_services, len(_SERVICES)))
    trace_id = generate_trace_id()
    spans: list[Span] = []

    def _build(parent_id: Optional[str], depth: int, t_start: float) -> float:
        if depth > max_depth or not services:
            return t_start
        svc_name, ops = random.choice(services)
        op = random.choice(ops)
        span_id = generate_span_id()
        self_time = random.uniform(base_latency_ms * 0.5, base_latency_ms * 2.0) / 1000.0

        t_cursor = t_start + self_time * 0.3  # some self-work before children
        n_children = random.randint(0, min(3, max_depth - depth))
        for _ in range(n_children):
            t_cursor = _build(span_id, depth + 1, t_cursor)
            t_cursor += random.uniform(0.0001, 0.001)

        t_end = t_cursor + self_time * 0.7  # remaining self-work after children

        kind = SpanKind.SERVER if depth == 0 else SpanKind.CLIENT
        status = StatusCode.OK if random.random() > 0.05 else StatusCode.ERROR

        span = Span(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_id,
            operation_name=op,
            service_name=svc_name,
            kind=kind,
            start_time=t_start,
            end_time=t_end,
            status=status,
        )
        span.set_tag("service", svc_name)
        if status == StatusCode.ERROR:
            span.set_tag("error", True)
            span.add_event("exception", {"message": "simulated error"})
        spans.append(span)
        return t_end

    _build(None, 0, time.time())
    random.shuffle(spans)  # simulate out-of-order arrival
    return spans


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Trace assembly demo")
    parser.add_argument("--services", type=int, default=5, help="Services per trace")
    parser.add_argument("--depth", type=int, default=4, help="Max span tree depth")
    parser.add_argument("--traces", type=int, default=3, help="Number of traces")
    args = parser.parse_args()

    assembler = TraceAssembler()
    for _ in range(args.traces):
        spans = generate_trace(n_services=args.services, max_depth=args.depth)
        assembler.add_spans(spans)

    traces = assembler.assemble_all()
    for i, trace in enumerate(traces):
        print(f"\n{'='*60}")
        print(f"Trace {i+1}: {trace.trace_id[:16]}… "
              f"({trace.span_count} spans, {trace.duration_ms:.1f}ms)")
        print(f"Services: {', '.join(sorted(trace.service_names))}")
        print(f"{'='*60}")
        print(trace.print_tree())
