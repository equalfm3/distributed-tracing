"""Trace context: trace ID, span ID, sampling flag, and baggage items.

The trace context carries identity and metadata across service boundaries.
It is the fundamental unit of context propagation — every outgoing call
injects the context, and every incoming call extracts it.
"""

from __future__ import annotations

import os
import struct
import time
from dataclasses import dataclass, field
from typing import Optional


def _random_hex(n_bytes: int) -> str:
    """Generate a random hex string of *n_bytes* bytes (2*n_bytes chars)."""
    return os.urandom(n_bytes).hex()


def generate_trace_id() -> str:
    """Generate a 128-bit (32-hex-char) trace ID.

    Uses 8 bytes of timestamp prefix for rough ordering plus 8 random bytes
    to avoid collisions, matching the W3C Trace Context format.

    Returns:
        A 32-character lowercase hex string.
    """
    ts_bytes = struct.pack(">Q", int(time.time() * 1e6) & 0xFFFFFFFFFFFFFFFF)
    rand_bytes = os.urandom(8)
    return (ts_bytes + rand_bytes).hex()


def generate_span_id() -> str:
    """Generate a 64-bit (16-hex-char) span ID.

    Returns:
        A 16-character lowercase hex string.
    """
    return _random_hex(8)


# ---------------------------------------------------------------------------
# Trace flags
# ---------------------------------------------------------------------------
TRACE_FLAG_SAMPLED = 0x01


@dataclass
class TraceContext:
    """Immutable snapshot of the current trace context.

    Attributes:
        trace_id: 32-hex-char trace identifier shared by all spans in a trace.
        span_id: 16-hex-char identifier for the current span.
        trace_flags: Bit field — bit 0 is the *sampled* flag.
        trace_state: Vendor-specific key-value pairs (W3C ``tracestate``).
        baggage: Application-level key-value pairs propagated across services.
    """

    trace_id: str
    span_id: str
    trace_flags: int = TRACE_FLAG_SAMPLED
    trace_state: dict[str, str] = field(default_factory=dict)
    baggage: dict[str, str] = field(default_factory=dict)

    # -- convenience properties -------------------------------------------

    @property
    def is_sampled(self) -> bool:
        """Return True if the sampled flag is set."""
        return bool(self.trace_flags & TRACE_FLAG_SAMPLED)

    @property
    def trace_id_bytes(self) -> bytes:
        """Return the trace ID as raw bytes."""
        return bytes.fromhex(self.trace_id)

    @property
    def span_id_bytes(self) -> bytes:
        """Return the span ID as raw bytes."""
        return bytes.fromhex(self.span_id)

    # -- factory helpers --------------------------------------------------

    @classmethod
    def new_root(cls, sampled: bool = True) -> TraceContext:
        """Create a brand-new root context (new trace + span IDs).

        Args:
            sampled: Whether this trace should be sampled.

        Returns:
            A fresh TraceContext with unique IDs.
        """
        flags = TRACE_FLAG_SAMPLED if sampled else 0
        return cls(
            trace_id=generate_trace_id(),
            span_id=generate_span_id(),
            trace_flags=flags,
        )

    def child(self) -> TraceContext:
        """Derive a child context (same trace ID, new span ID).

        Returns:
            A new TraceContext sharing the trace ID but with a fresh span ID.
        """
        return TraceContext(
            trace_id=self.trace_id,
            span_id=generate_span_id(),
            trace_flags=self.trace_flags,
            trace_state=dict(self.trace_state),
            baggage=dict(self.baggage),
        )

    # -- baggage helpers --------------------------------------------------

    def with_baggage(self, key: str, value: str) -> TraceContext:
        """Return a new context with an additional baggage item.

        Args:
            key: Baggage key.
            value: Baggage value.

        Returns:
            A copy of this context with the baggage item added.
        """
        new_baggage = dict(self.baggage)
        new_baggage[key] = value
        return TraceContext(
            trace_id=self.trace_id,
            span_id=self.span_id,
            trace_flags=self.trace_flags,
            trace_state=dict(self.trace_state),
            baggage=new_baggage,
        )

    # -- serialisation ----------------------------------------------------

    def __repr__(self) -> str:
        sampled = "sampled" if self.is_sampled else "not-sampled"
        return (
            f"TraceContext(trace={self.trace_id[:8]}…, "
            f"span={self.span_id[:8]}…, {sampled})"
        )


# -----------------------------------------------------------------------
# Context stack — thread-local-style context management
# -----------------------------------------------------------------------

_context_stack: list[Optional[TraceContext]] = []


def attach(ctx: TraceContext) -> int:
    """Push *ctx* onto the context stack and return a token for detach.

    Args:
        ctx: The trace context to make current.

    Returns:
        An integer token used to restore the previous context.
    """
    _context_stack.append(ctx)
    return len(_context_stack) - 1


def detach(token: int) -> None:
    """Restore the context stack to the state before *attach* at *token*.

    Args:
        token: The token returned by a prior ``attach`` call.
    """
    while len(_context_stack) > token:
        _context_stack.pop()


def current_context() -> Optional[TraceContext]:
    """Return the current active trace context, or None."""
    return _context_stack[-1] if _context_stack else None


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Trace Context Demo ===\n")

    root = TraceContext.new_root(sampled=True)
    print(f"Root context : {root}")
    print(f"  trace_id   : {root.trace_id}")
    print(f"  span_id    : {root.span_id}")
    print(f"  sampled    : {root.is_sampled}")

    child = root.child()
    print(f"\nChild context: {child}")
    print(f"  same trace : {child.trace_id == root.trace_id}")
    print(f"  new span   : {child.span_id != root.span_id}")

    ctx_with_bag = root.with_baggage("user_id", "u-42")
    print(f"\nWith baggage : {ctx_with_bag.baggage}")

    token = attach(root)
    print(f"\nAttached root: current = {current_context()}")
    token2 = attach(child)
    print(f"Attached child: current = {current_context()}")
    detach(token2)
    print(f"Detached child: current = {current_context()}")
    detach(token)
    print(f"Detached root : current = {current_context()}")
