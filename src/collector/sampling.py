"""Sampling strategies: head-based and tail-based sampling.

Head-based sampling decides at the root span using a deterministic hash of
the trace ID — all services make the same decision for the same trace.
Tail-based sampling defers the decision until the trace is complete, keeping
interesting traces (errors, high latency) and discarding boring ones.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ..trace.span import Span, StatusCode


class Sampler(ABC):
    """Base class for sampling strategies."""

    @abstractmethod
    def should_sample(self, trace_id: str, spans: Optional[list[Span]] = None) -> bool:
        """Decide whether a trace should be retained.

        Args:
            trace_id: The trace identifier.
            spans: Complete span list (used by tail-based samplers).

        Returns:
            True if the trace should be kept.
        """
        ...


class AlwaysSampler(Sampler):
    """Sample every trace — useful for debugging and low-traffic systems."""

    def should_sample(self, trace_id: str, spans: Optional[list[Span]] = None) -> bool:
        """Always returns True."""
        return True


class NeverSampler(Sampler):
    """Drop every trace — useful for disabling tracing without code changes."""

    def should_sample(self, trace_id: str, spans: Optional[list[Span]] = None) -> bool:
        """Always returns False."""
        return False


@dataclass
class RateSampler(Sampler):
    """Head-based probabilistic sampler using a deterministic hash.

    The hash of the trace ID is compared against a threshold derived from
    the sampling rate.  This ensures all services make the same decision
    for the same trace — no partial traces.

    Attributes:
        rate: Sampling probability in [0.0, 1.0].
    """

    rate: float = 0.1

    def __post_init__(self) -> None:
        if not 0.0 <= self.rate <= 1.0:
            raise ValueError(f"Sampling rate must be in [0, 1], got {self.rate}")

    def _hash_trace_id(self, trace_id: str) -> float:
        """Map a trace ID to a uniform float in [0, 1).

        Args:
            trace_id: The trace identifier.

        Returns:
            A deterministic float in [0, 1).
        """
        digest = hashlib.sha256(trace_id.encode()).digest()
        value = int.from_bytes(digest[:8], "big")
        return value / (2**64)

    def should_sample(self, trace_id: str, spans: Optional[list[Span]] = None) -> bool:
        """Sample based on deterministic hash of trace ID.

        Args:
            trace_id: The trace identifier.
            spans: Ignored for head-based sampling.

        Returns:
            True if hash(trace_id) < rate.
        """
        return self._hash_trace_id(trace_id) < self.rate


@dataclass
class TailSampler(Sampler):
    """Tail-based sampler that keeps interesting traces.

    Retains traces that contain errors, exceed a latency threshold, or
    match specific operation patterns.  Falls back to rate-based sampling
    for uninteresting traces.

    Attributes:
        latency_threshold_ms: Keep traces slower than this.
        keep_errors: Always keep traces with error spans.
        fallback_rate: Rate-sample uninteresting traces.
        keep_operations: Always keep traces containing these operations.
    """

    latency_threshold_ms: float = 500.0
    keep_errors: bool = True
    fallback_rate: float = 0.01
    keep_operations: list[str] = field(default_factory=list)

    def should_sample(self, trace_id: str, spans: Optional[list[Span]] = None) -> bool:
        """Decide based on trace characteristics.

        Args:
            trace_id: The trace identifier.
            spans: The complete list of spans for this trace.

        Returns:
            True if the trace is interesting or passes fallback sampling.
        """
        if not spans:
            return RateSampler(self.fallback_rate).should_sample(trace_id)

        # Keep traces with errors
        if self.keep_errors:
            if any(s.status == StatusCode.ERROR for s in spans):
                return True

        # Keep slow traces
        root_spans = [s for s in spans if s.parent_span_id is None]
        if root_spans:
            max_duration = max(s.duration_ms for s in root_spans)
            if max_duration > self.latency_threshold_ms:
                return True

        # Keep traces with specific operations
        if self.keep_operations:
            ops = {s.operation_name for s in spans}
            if ops & set(self.keep_operations):
                return True

        # Fallback to rate sampling
        return RateSampler(self.fallback_rate).should_sample(trace_id)


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    from ..trace.trace import generate_trace
    from ..trace.context import generate_trace_id

    print("=== Sampling Strategies Demo ===\n")

    # Head-based rate sampling
    rate_sampler = RateSampler(rate=0.1)
    n_total, n_sampled = 10000, 0
    for _ in range(n_total):
        tid = generate_trace_id()
        if rate_sampler.should_sample(tid):
            n_sampled += 1
    print(f"Rate sampler (10%): {n_sampled}/{n_total} sampled "
          f"({n_sampled/n_total*100:.1f}%)")

    # Determinism check
    tid = generate_trace_id()
    results = [rate_sampler.should_sample(tid) for _ in range(100)]
    print(f"Deterministic: all same = {len(set(results)) == 1}")

    # Tail-based sampling
    tail_sampler = TailSampler(
        latency_threshold_ms=50.0,
        keep_errors=True,
        fallback_rate=0.01,
    )
    n_traces, n_kept = 100, 0
    for _ in range(n_traces):
        spans = generate_trace(n_services=4, max_depth=3)
        tid = spans[0].trace_id
        if tail_sampler.should_sample(tid, spans):
            n_kept += 1
    print(f"\nTail sampler: {n_kept}/{n_traces} kept "
          f"({n_kept/n_traces*100:.1f}%)")
    print("  (keeps errors + slow traces + fallback 1%)")
