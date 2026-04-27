"""Composite propagator supporting multiple propagation formats.

In real systems, services may use different propagation formats (W3C, B3,
Jaeger, etc.).  The composite propagator tries each format in order during
extraction and can inject into multiple formats simultaneously for
interoperability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol

from ..trace.context import TraceContext
from .w3c import W3CPropagator
from .b3 import B3Propagator


class Propagator(Protocol):
    """Protocol for context propagators."""

    def inject(self, ctx: TraceContext, carrier: dict[str, str]) -> None:
        """Inject context into a carrier."""
        ...

    def extract(self, carrier: dict[str, str]) -> Optional[TraceContext]:
        """Extract context from a carrier."""
        ...


@dataclass
class CompositePropagator:
    """Tries multiple propagation formats for extraction and injection.

    During extraction, formats are tried in order — the first successful
    extraction wins.  During injection, context is injected into all
    formats for maximum interoperability.

    Attributes:
        propagators: Ordered list of propagators to try.
    """

    propagators: list[Propagator] = field(default_factory=list)

    def inject(self, ctx: TraceContext, carrier: dict[str, str]) -> None:
        """Inject context using all propagators.

        Args:
            ctx: The trace context to propagate.
            carrier: Mutable header dict.
        """
        for p in self.propagators:
            p.inject(ctx, carrier)

    def extract(self, carrier: dict[str, str]) -> Optional[TraceContext]:
        """Extract context using the first successful propagator.

        Args:
            carrier: Header dict to extract from.

        Returns:
            A TraceContext from the first matching format, or None.
        """
        for p in self.propagators:
            ctx = p.extract(carrier)
            if ctx is not None:
                return ctx
        return None


def default_propagator() -> CompositePropagator:
    """Create a propagator that supports W3C (primary) and B3 (fallback).

    Returns:
        A CompositePropagator with W3C and B3 support.
    """
    return CompositePropagator(
        propagators=[
            W3CPropagator(),
            B3Propagator(use_single_header=False),
            B3Propagator(use_single_header=True),
        ]
    )


def simulate_service_call(
    propagator: CompositePropagator,
    caller_ctx: TraceContext,
    callee_name: str,
) -> tuple[dict[str, str], Optional[TraceContext]]:
    """Simulate a cross-service call with context propagation.

    Args:
        propagator: The propagator to use.
        caller_ctx: The caller's trace context.
        callee_name: Name of the callee service (for display).

    Returns:
        Tuple of (headers, extracted_context).
    """
    headers: dict[str, str] = {}
    propagator.inject(caller_ctx, headers)
    extracted = propagator.extract(headers)
    return headers, extracted


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Composite Propagator Demo ===\n")

    prop = default_propagator()

    # Simulate a chain of service calls
    services = ["api-gateway", "auth-service", "user-service", "db-proxy"]
    ctx = TraceContext.new_root(sampled=True)

    print(f"Trace ID: {ctx.trace_id[:16]}…\n")

    for i, svc in enumerate(services):
        headers, extracted = simulate_service_call(prop, ctx, svc)

        print(f"[{svc}]")
        print(f"  Headers injected ({len(headers)}):")
        for k, v in sorted(headers.items()):
            val_display = v if len(v) < 60 else v[:57] + "…"
            print(f"    {k}: {val_display}")

        if extracted:
            print(f"  Extracted: trace={extracted.trace_id[:16]}… "
                  f"span={extracted.span_id[:8]}… "
                  f"sampled={extracted.is_sampled}")
            # Create child context for next hop
            ctx = extracted.child()
        print()

    # Test W3C-only extraction
    print("--- Format-specific extraction ---")
    w3c_only: dict[str, str] = {}
    W3CPropagator().inject(ctx, w3c_only)
    result = prop.extract(w3c_only)
    print(f"W3C headers → extracted: {result is not None}")

    # Test B3-only extraction
    b3_only: dict[str, str] = {}
    B3Propagator(use_single_header=True).inject(ctx, b3_only)
    result = prop.extract(b3_only)
    print(f"B3 single header → extracted: {result is not None}")

    # Test empty headers
    result = prop.extract({})
    print(f"Empty headers → extracted: {result is not None}")
