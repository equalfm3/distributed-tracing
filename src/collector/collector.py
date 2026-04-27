"""Span collector: receives, validates, batches, and exports spans.

The collector is the central hub of the tracing pipeline.  It receives
spans from instrumented services, validates them, applies sampling, batches
them for efficiency, and forwards them to one or more exporters.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from ..trace.span import Span, StatusCode
from .sampling import Sampler, AlwaysSampler, RateSampler
from .exporter import SpanExporter, InMemoryExporter


@dataclass
class CollectorStats:
    """Running statistics for the collector.

    Attributes:
        received: Total spans received.
        accepted: Spans that passed validation and sampling.
        rejected: Spans that failed validation.
        dropped_sampling: Spans dropped by the sampler.
        exported: Spans successfully exported.
        batches_flushed: Number of batch flushes.
    """

    received: int = 0
    accepted: int = 0
    rejected: int = 0
    dropped_sampling: int = 0
    exported: int = 0
    batches_flushed: int = 0

    def summary(self) -> str:
        """Return a one-line summary of collector stats."""
        return (
            f"received={self.received} accepted={self.accepted} "
            f"rejected={self.rejected} dropped={self.dropped_sampling} "
            f"exported={self.exported} flushes={self.batches_flushed}"
        )


class ValidationError(Exception):
    """Raised when a span fails validation."""


def validate_span(span: Span) -> list[str]:
    """Validate a span and return a list of error messages.

    Args:
        span: The span to validate.

    Returns:
        List of validation error strings (empty if valid).
    """
    errors: list[str] = []
    if not span.trace_id or len(span.trace_id) != 32:
        errors.append(f"Invalid trace_id length: {len(span.trace_id) if span.trace_id else 0}")
    if not span.span_id or len(span.span_id) != 16:
        errors.append(f"Invalid span_id length: {len(span.span_id) if span.span_id else 0}")
    if not span.operation_name:
        errors.append("Missing operation_name")
    if not span.service_name:
        errors.append("Missing service_name")
    if span.end_time is not None and span.end_time < span.start_time:
        errors.append("end_time < start_time")
    return errors


@dataclass
class SpanCollector:
    """Central span collector with validation, sampling, and batched export.

    Attributes:
        sampler: Sampling strategy.
        exporter: Span export backend.
        batch_size: Number of spans to buffer before flushing.
        stats: Running statistics.
    """

    sampler: Sampler = field(default_factory=AlwaysSampler)
    exporter: SpanExporter = field(default_factory=InMemoryExporter)
    batch_size: int = 100
    stats: CollectorStats = field(default_factory=CollectorStats)
    _buffer: list[Span] = field(default_factory=list, init=False)
    _sampling_cache: dict[str, bool] = field(default_factory=dict, init=False)

    def receive(self, span: Span) -> bool:
        """Receive and process a single span.

        Validates the span, applies sampling, and buffers it for export.
        Flushes the buffer when it reaches ``batch_size``.

        Args:
            span: The span to process.

        Returns:
            True if the span was accepted.
        """
        self.stats.received += 1

        # Validate
        errors = validate_span(span)
        if errors:
            self.stats.rejected += 1
            return False

        # Sample (cache decision per trace_id for consistency)
        if span.trace_id not in self._sampling_cache:
            self._sampling_cache[span.trace_id] = self.sampler.should_sample(span.trace_id)
        if not self._sampling_cache[span.trace_id]:
            self.stats.dropped_sampling += 1
            return False

        self.stats.accepted += 1
        self._buffer.append(span)

        if len(self._buffer) >= self.batch_size:
            self.flush()

        return True

    def receive_batch(self, spans: list[Span]) -> int:
        """Receive multiple spans.

        Args:
            spans: Spans to process.

        Returns:
            Number of spans accepted.
        """
        return sum(1 for s in spans if self.receive(s))

    def flush(self) -> int:
        """Flush the buffer to the exporter.

        Returns:
            Number of spans exported.
        """
        if not self._buffer:
            return 0
        n = self.exporter.export(self._buffer)
        self.stats.exported += n
        self.stats.batches_flushed += 1
        self._buffer.clear()
        return n

    def shutdown(self) -> None:
        """Flush remaining spans and shut down the exporter."""
        self.flush()
        self.exporter.shutdown()


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from ..trace.trace import generate_trace

    parser = argparse.ArgumentParser(description="Span collector demo")
    parser.add_argument("--sampling-rate", type=float, default=0.5,
                        help="Head-based sampling rate")
    parser.add_argument("--traces", type=int, default=50,
                        help="Number of traces to generate")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="Batch size for export")
    args = parser.parse_args()

    print("=== Span Collector Demo ===\n")

    mem_exporter = InMemoryExporter()
    collector = SpanCollector(
        sampler=RateSampler(rate=args.sampling_rate),
        exporter=mem_exporter,
        batch_size=args.batch_size,
    )

    total_spans = 0
    for _ in range(args.traces):
        spans = generate_trace(n_services=4, max_depth=3)
        total_spans += len(spans)
        collector.receive_batch(spans)

    collector.flush()

    print(f"Generated {total_spans} spans across {args.traces} traces")
    print(f"Collector stats: {collector.stats.summary()}")
    print(f"Stored traces: {len(mem_exporter.trace_ids)}")
    print(f"Stored spans : {len(mem_exporter.spans)}")
    print(f"\nSampling rate: {args.sampling_rate:.0%}")
    actual = collector.stats.accepted / max(collector.stats.received, 1)
    print(f"Actual accept: {actual:.1%}")
