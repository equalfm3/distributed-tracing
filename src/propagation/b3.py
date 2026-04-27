"""B3 (Zipkin) propagation format: injection and extraction.

B3 propagation uses multiple HTTP headers (multi-header format) or a single
combined header (single-header format).  This module supports both.

Multi-header format:
    X-B3-TraceId: {trace_id}
    X-B3-SpanId: {span_id}
    X-B3-ParentSpanId: {parent_span_id}
    X-B3-Sampled: {0|1}

Single-header format:
    b3: {trace_id}-{span_id}-{sampling_state}-{parent_span_id}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..trace.context import TraceContext, TRACE_FLAG_SAMPLED


# Header keys for multi-header B3
B3_TRACE_ID = "x-b3-traceid"
B3_SPAN_ID = "x-b3-spanid"
B3_PARENT_SPAN_ID = "x-b3-parentspanid"
B3_SAMPLED = "x-b3-sampled"
B3_FLAGS = "x-b3-flags"

# Single-header key
B3_SINGLE = "b3"


@dataclass
class B3Context:
    """Parsed B3 propagation context.

    Attributes:
        trace_id: 16 or 32 hex char trace identifier.
        span_id: 16 hex char span identifier.
        parent_span_id: Optional parent span ID.
        sampled: Whether the trace is sampled.
        debug: Whether debug flag is set (forces sampling).
    """

    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    sampled: bool = True
    debug: bool = False

    def to_single_header(self) -> str:
        """Serialise to the single-header ``b3`` format.

        Returns:
            A B3 single-header string.
        """
        sampling = "d" if self.debug else ("1" if self.sampled else "0")
        parts = [self.trace_id, self.span_id, sampling]
        if self.parent_span_id:
            parts.append(self.parent_span_id)
        return "-".join(parts)

    @classmethod
    def from_single_header(cls, value: str) -> Optional[B3Context]:
        """Parse a single-header ``b3`` value.

        Args:
            value: The raw header string.

        Returns:
            A B3Context, or None if parsing fails.
        """
        value = value.strip()
        # Handle deny (sampling=0 shorthand)
        if value == "0":
            return None

        parts = value.split("-")
        if len(parts) < 2:
            return None

        trace_id = parts[0]
        span_id = parts[1]

        if len(trace_id) not in (16, 32) or len(span_id) != 16:
            return None

        sampled = True
        debug = False
        parent_span_id = None

        if len(parts) >= 3:
            flag = parts[2]
            if flag == "d":
                debug = True
            elif flag == "0":
                sampled = False
            # "1" or empty → sampled=True

        if len(parts) >= 4:
            parent_span_id = parts[3]

        return cls(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            sampled=sampled,
            debug=debug,
        )


class B3Propagator:
    """Inject and extract B3 context from carrier dicts.

    Supports both multi-header and single-header formats.

    Attributes:
        use_single_header: If True, use the single ``b3`` header.
    """

    def __init__(self, use_single_header: bool = False) -> None:
        self.use_single_header = use_single_header

    def inject(self, ctx: TraceContext, carrier: dict[str, str]) -> None:
        """Inject B3 context into outgoing request headers.

        Args:
            ctx: The current trace context.
            carrier: Mutable header dict.
        """
        b3 = B3Context(
            trace_id=ctx.trace_id,
            span_id=ctx.span_id,
            sampled=ctx.is_sampled,
        )

        if self.use_single_header:
            carrier[B3_SINGLE] = b3.to_single_header()
        else:
            carrier[B3_TRACE_ID] = ctx.trace_id
            carrier[B3_SPAN_ID] = ctx.span_id
            carrier[B3_SAMPLED] = "1" if ctx.is_sampled else "0"

    def extract(self, carrier: dict[str, str]) -> Optional[TraceContext]:
        """Extract B3 context from incoming request headers.

        Tries single-header first, then falls back to multi-header.

        Args:
            carrier: Header dict (keys should be lowercase).

        Returns:
            A TraceContext, or None if no B3 headers found.
        """
        # Normalise keys to lowercase
        lower = {k.lower(): v for k, v in carrier.items()}

        # Try single header first
        if B3_SINGLE in lower:
            b3 = B3Context.from_single_header(lower[B3_SINGLE])
            if b3:
                flags = TRACE_FLAG_SAMPLED if b3.sampled else 0
                return TraceContext(
                    trace_id=b3.trace_id,
                    span_id=b3.span_id,
                    trace_flags=flags,
                )

        # Multi-header fallback
        trace_id = lower.get(B3_TRACE_ID)
        span_id = lower.get(B3_SPAN_ID)
        if not trace_id or not span_id:
            return None

        # Pad 64-bit trace IDs to 128-bit
        if len(trace_id) == 16:
            trace_id = "0" * 16 + trace_id

        sampled_raw = lower.get(B3_SAMPLED, "1")
        debug = lower.get(B3_FLAGS) == "1"
        sampled = debug or sampled_raw == "1"
        flags = TRACE_FLAG_SAMPLED if sampled else 0

        return TraceContext(
            trace_id=trace_id,
            span_id=span_id,
            trace_flags=flags,
        )


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    print("=== B3 Propagation Demo ===\n")

    ctx = TraceContext.new_root(sampled=True)

    # Multi-header format
    multi = B3Propagator(use_single_header=False)
    headers: dict[str, str] = {}
    multi.inject(ctx, headers)
    print("Multi-header format:")
    for k, v in sorted(headers.items()):
        print(f"  {k}: {v}")

    extracted = multi.extract(headers)
    assert extracted is not None
    assert extracted.trace_id == ctx.trace_id
    print(f"  Round-trip: OK (trace={extracted.trace_id[:16]}…)")

    # Single-header format
    single = B3Propagator(use_single_header=True)
    headers2: dict[str, str] = {}
    single.inject(ctx, headers2)
    print(f"\nSingle-header format:")
    print(f"  b3: {headers2['b3']}")

    extracted2 = single.extract(headers2)
    assert extracted2 is not None
    assert extracted2.trace_id == ctx.trace_id
    print(f"  Round-trip: OK (trace={extracted2.trace_id[:16]}…)")

    # Parse examples
    print("\n--- Parsing examples ---")
    examples = [
        "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1",
        "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-d",
        "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-0-05e3ac9a4f6e3b90",
    ]
    for ex in examples:
        b3 = B3Context.from_single_header(ex)
        if b3:
            print(f"  {ex}")
            print(f"    trace={b3.trace_id[:16]}… sampled={b3.sampled} debug={b3.debug}")
