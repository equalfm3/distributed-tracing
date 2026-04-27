"""Span data model: trace ID, span ID, parent, tags, logs, and timing.

A span is the fundamental unit of distributed tracing.  It records a single
unit of work — an HTTP handler, a database query, a cache lookup — with
precise timing, causal linkage (parent span), and arbitrary metadata.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from .context import TraceContext, generate_span_id


class SpanKind(Enum):
    """Classifies the relationship between the span and the remote side."""

    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class StatusCode(Enum):
    """Span completion status."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass
class SpanEvent:
    """A timestamped annotation within a span.

    Attributes:
        name: Short description of the event.
        timestamp: Unix epoch seconds (float).
        attributes: Arbitrary key-value metadata.
    """

    name: str
    timestamp: float = field(default_factory=time.time)
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class SpanLink:
    """A causal link to another span (e.g. a batch trigger).

    Attributes:
        trace_id: Linked span's trace ID.
        span_id: Linked span's span ID.
        attributes: Metadata about the link.
    """

    trace_id: str
    span_id: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Span:
    """A single unit of work in a distributed trace.

    Attributes:
        trace_id: 32-hex-char trace identifier.
        span_id: 16-hex-char span identifier.
        parent_span_id: Parent span ID (None for root spans).
        operation_name: Human-readable name (e.g. ``GET /api/users``).
        service_name: The service that produced this span.
        kind: Relationship to the remote side.
        start_time: Unix epoch seconds when the span started.
        end_time: Unix epoch seconds when the span ended (None while active).
        tags: Key-value metadata.
        events: Timestamped annotations.
        links: Causal links to other spans.
        status: Completion status.
        status_message: Optional message for error status.
    """

    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    service_name: str
    kind: SpanKind = SpanKind.INTERNAL
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    tags: dict[str, Any] = field(default_factory=dict)
    events: list[SpanEvent] = field(default_factory=list)
    links: list[SpanLink] = field(default_factory=list)
    status: StatusCode = StatusCode.UNSET
    status_message: Optional[str] = None

    # -- lifecycle --------------------------------------------------------

    def finish(self, end_time: Optional[float] = None) -> None:
        """Mark the span as finished.

        Args:
            end_time: Explicit end time; defaults to ``time.time()``.
        """
        self.end_time = end_time or time.time()

    @property
    def duration_ms(self) -> float:
        """Duration in milliseconds (0.0 if not finished)."""
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time) * 1000.0

    @property
    def is_root(self) -> bool:
        """True if this span has no parent."""
        return self.parent_span_id is None

    # -- mutation helpers -------------------------------------------------

    def set_tag(self, key: str, value: Any) -> Span:
        """Add or overwrite a tag.

        Args:
            key: Tag key.
            value: Tag value.

        Returns:
            self (for chaining).
        """
        self.tags[key] = value
        return self

    def add_event(self, name: str, attributes: Optional[dict[str, Any]] = None) -> Span:
        """Record a timestamped event.

        Args:
            name: Event name.
            attributes: Optional event metadata.

        Returns:
            self (for chaining).
        """
        self.events.append(SpanEvent(name=name, attributes=attributes or {}))
        return self

    def set_status(self, code: StatusCode, message: Optional[str] = None) -> Span:
        """Set the span's completion status.

        Args:
            code: Status code.
            message: Optional description (typically for errors).

        Returns:
            self (for chaining).
        """
        self.status = code
        self.status_message = message
        return self

    # -- factory ----------------------------------------------------------

    @classmethod
    def from_context(
        cls,
        ctx: TraceContext,
        operation_name: str,
        service_name: str,
        parent_span_id: Optional[str] = None,
        kind: SpanKind = SpanKind.INTERNAL,
    ) -> Span:
        """Create a span from an existing trace context.

        Args:
            ctx: The active trace context.
            operation_name: Human-readable operation name.
            service_name: Originating service.
            parent_span_id: Explicit parent; defaults to ``ctx.span_id``.
            kind: Span kind.

        Returns:
            A new Span linked to the context.
        """
        return cls(
            trace_id=ctx.trace_id,
            span_id=generate_span_id(),
            parent_span_id=parent_span_id or ctx.span_id,
            operation_name=operation_name,
            service_name=service_name,
            kind=kind,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialise the span to a plain dictionary.

        Returns:
            A JSON-friendly dictionary representation.
        """
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "service_name": self.service_name,
            "kind": self.kind.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "tags": self.tags,
            "events": [
                {"name": e.name, "timestamp": e.timestamp, "attributes": e.attributes}
                for e in self.events
            ],
            "status": self.status.value,
            "status_message": self.status_message,
        }

    def __repr__(self) -> str:
        dur = f"{self.duration_ms:.1f}ms" if self.end_time else "active"
        return (
            f"Span({self.operation_name!r}, service={self.service_name!r}, "
            f"{dur}, id={self.span_id[:8]}…)"
        )


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    from .context import TraceContext

    print("=== Span Data Model Demo ===\n")

    ctx = TraceContext.new_root()
    root = Span(
        trace_id=ctx.trace_id,
        span_id=ctx.span_id,
        parent_span_id=None,
        operation_name="GET /api/users",
        service_name="api-gateway",
        kind=SpanKind.SERVER,
    )
    root.set_tag("http.method", "GET").set_tag("http.url", "/api/users")
    root.add_event("request_received", {"client_ip": "10.0.0.1"})

    child = Span.from_context(
        ctx, "SELECT * FROM users", "user-service", kind=SpanKind.CLIENT
    )
    child.set_tag("db.type", "postgresql").set_tag("db.statement", "SELECT * FROM users")

    import time as _t

    _t.sleep(0.01)
    child.finish()
    child.set_status(StatusCode.OK)

    _t.sleep(0.005)
    root.finish()
    root.set_status(StatusCode.OK)

    print(f"Root span : {root}")
    print(f"Child span: {child}")
    print(f"\nRoot dict keys: {list(root.to_dict().keys())}")
    print(f"Child duration : {child.duration_ms:.2f} ms")
    print(f"Root is_root   : {root.is_root}")
    print(f"Child is_root  : {child.is_root}")
