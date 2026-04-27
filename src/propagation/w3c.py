"""W3C Trace Context injection and extraction.

Implements the W3C Trace Context specification (https://www.w3.org/TR/trace-context/)
for propagating trace identity across service boundaries via HTTP headers.

Headers:
    traceparent: ``{version}-{trace_id}-{parent_id}-{trace_flags}``
    tracestate:  Vendor-specific key-value pairs (comma-separated).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from ..trace.context import TraceContext, TRACE_FLAG_SAMPLED

# W3C traceparent format: version-trace_id-parent_id-trace_flags
# Example: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
_TRACEPARENT_RE = re.compile(
    r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)

# tracestate: key=value pairs separated by commas
# key: [a-z][a-z0-9_\-*/]{0,255} or tenant@system format
_TRACESTATE_KEY_RE = re.compile(
    r"^[a-z][a-z0-9_\-*/]{0,255}(@[a-z][a-z0-9_\-*/]{0,13})?$"
)
_TRACESTATE_MAX_MEMBERS = 32

W3C_VERSION = "00"


@dataclass
class W3CTraceParent:
    """Parsed W3C traceparent header.

    Attributes:
        version: Format version (always "00" for current spec).
        trace_id: 32-hex-char trace identifier.
        parent_id: 16-hex-char parent span identifier.
        trace_flags: 2-hex-char flags (bit 0 = sampled).
    """

    version: str
    trace_id: str
    parent_id: str
    trace_flags: int

    def to_header(self) -> str:
        """Serialise to the ``traceparent`` header value.

        Returns:
            A W3C-compliant traceparent string.
        """
        return f"{self.version}-{self.trace_id}-{self.parent_id}-{self.trace_flags:02x}"

    @classmethod
    def from_header(cls, value: str) -> Optional[W3CTraceParent]:
        """Parse a ``traceparent`` header value.

        Args:
            value: The raw header string.

        Returns:
            A W3CTraceParent instance, or None if parsing fails.
        """
        value = value.strip().lower()
        m = _TRACEPARENT_RE.match(value)
        if not m:
            return None
        version, trace_id, parent_id, flags_hex = m.groups()
        # Reject all-zero trace_id or parent_id
        if trace_id == "0" * 32 or parent_id == "0" * 16:
            return None
        return cls(
            version=version,
            trace_id=trace_id,
            parent_id=parent_id,
            trace_flags=int(flags_hex, 16),
        )


def parse_tracestate(header: str) -> dict[str, str]:
    """Parse a ``tracestate`` header into a dict.

    Args:
        header: Comma-separated key=value pairs.

    Returns:
        Ordered dict of vendor entries (max 32 members).
    """
    result: dict[str, str] = {}
    if not header.strip():
        return result
    for entry in header.split(","):
        entry = entry.strip()
        if "=" not in entry:
            continue
        key, _, value = entry.partition("=")
        key = key.strip()
        value = value.strip()
        if _TRACESTATE_KEY_RE.match(key) and len(result) < _TRACESTATE_MAX_MEMBERS:
            result[key] = value
    return result


def format_tracestate(state: dict[str, str]) -> str:
    """Serialise a tracestate dict to a header value.

    Args:
        state: Vendor key-value pairs.

    Returns:
        Comma-separated header string.
    """
    return ",".join(f"{k}={v}" for k, v in state.items())


class W3CPropagator:
    """Inject and extract W3C Trace Context from carrier dicts.

    A *carrier* is a dict[str, str] representing HTTP headers.
    """

    TRACEPARENT_KEY = "traceparent"
    TRACESTATE_KEY = "tracestate"

    def inject(self, ctx: TraceContext, carrier: dict[str, str]) -> None:
        """Inject trace context into a carrier (outgoing request headers).

        Args:
            ctx: The current trace context.
            carrier: Mutable header dict to inject into.
        """
        tp = W3CTraceParent(
            version=W3C_VERSION,
            trace_id=ctx.trace_id,
            parent_id=ctx.span_id,
            trace_flags=ctx.trace_flags,
        )
        carrier[self.TRACEPARENT_KEY] = tp.to_header()
        if ctx.trace_state:
            carrier[self.TRACESTATE_KEY] = format_tracestate(ctx.trace_state)

    def extract(self, carrier: dict[str, str]) -> Optional[TraceContext]:
        """Extract trace context from a carrier (incoming request headers).

        Args:
            carrier: Header dict to extract from.

        Returns:
            A TraceContext, or None if no valid traceparent is found.
        """
        raw = carrier.get(self.TRACEPARENT_KEY)
        if not raw:
            return None
        tp = W3CTraceParent.from_header(raw)
        if not tp:
            return None
        trace_state = {}
        if self.TRACESTATE_KEY in carrier:
            trace_state = parse_tracestate(carrier[self.TRACESTATE_KEY])
        return TraceContext(
            trace_id=tp.trace_id,
            span_id=tp.parent_id,
            trace_flags=tp.trace_flags,
            trace_state=trace_state,
        )


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="W3C Trace Context demo")
    parser.add_argument("--services", type=str, default="gateway,auth,user,db",
                        help="Comma-separated service names")
    parser.add_argument("--format", type=str, default="w3c", help="Propagation format")
    args = parser.parse_args()

    services = [s.strip() for s in args.services.split(",")]
    propagator = W3CPropagator()

    print("=== W3C Trace Context Propagation Demo ===\n")

    # Simulate request flowing through services
    ctx = TraceContext.new_root(sampled=True)
    ctx.trace_state["vendor1"] = "value1"

    for i, svc in enumerate(services):
        # Inject into outgoing headers
        headers: dict[str, str] = {}
        propagator.inject(ctx, headers)

        print(f"Service: {svc}")
        print(f"  Outgoing headers:")
        for k, v in headers.items():
            print(f"    {k}: {v}")

        # Next service extracts from incoming headers
        if i < len(services) - 1:
            extracted = propagator.extract(headers)
            if extracted:
                ctx = extracted.child()
                print(f"  Extracted trace_id: {extracted.trace_id[:16]}…")
                print(f"  New span_id: {ctx.span_id[:8]}…")
            print()

    # Round-trip test
    print("\n--- Round-trip validation ---")
    original = TraceContext.new_root()
    headers = {}
    propagator.inject(original, headers)
    recovered = propagator.extract(headers)
    assert recovered is not None
    assert recovered.trace_id == original.trace_id
    assert recovered.span_id == original.span_id
    print(f"traceparent round-trip: OK")
    print(f"  header: {headers['traceparent']}")
