"""Span export to storage backends: in-memory, JSON file, and console.

Exporters receive finished spans from the collector and persist them.
The in-memory exporter is used for analysis; the file exporter writes
newline-delimited JSON for offline processing.
"""

from __future__ import annotations

import json
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from ..trace.span import Span


class SpanExporter(ABC):
    """Base class for span exporters."""

    @abstractmethod
    def export(self, spans: list[Span]) -> int:
        """Export a batch of spans.

        Args:
            spans: Finished spans to export.

        Returns:
            Number of spans successfully exported.
        """
        ...

    def shutdown(self) -> None:
        """Clean up resources.  Override in subclasses if needed."""


class InMemoryExporter(SpanExporter):
    """Stores spans in memory for querying and analysis.

    Attributes:
        spans: All exported spans.
        _by_trace: Index from trace_id to span list.
    """

    def __init__(self) -> None:
        self.spans: list[Span] = []
        self._by_trace: dict[str, list[Span]] = {}

    def export(self, spans: list[Span]) -> int:
        """Append spans to the in-memory store.

        Args:
            spans: Spans to store.

        Returns:
            Number of spans stored.
        """
        for s in spans:
            self.spans.append(s)
            self._by_trace.setdefault(s.trace_id, []).append(s)
        return len(spans)

    def get_trace_spans(self, trace_id: str) -> list[Span]:
        """Retrieve all spans for a given trace.

        Args:
            trace_id: The trace identifier.

        Returns:
            List of spans (empty if trace not found).
        """
        return self._by_trace.get(trace_id, [])

    @property
    def trace_ids(self) -> list[str]:
        """All stored trace IDs."""
        return list(self._by_trace.keys())

    def clear(self) -> None:
        """Remove all stored spans."""
        self.spans.clear()
        self._by_trace.clear()


@dataclass
class JsonFileExporter(SpanExporter):
    """Writes spans as newline-delimited JSON to a file.

    Attributes:
        path: Output file path.
        _count: Number of spans written.
    """

    path: Path
    _count: int = field(default=0, init=False)

    def export(self, spans: list[Span]) -> int:
        """Append spans to the JSON file.

        Args:
            spans: Spans to write.

        Returns:
            Number of spans written.
        """
        with open(self.path, "a") as f:
            for s in spans:
                f.write(json.dumps(s.to_dict()) + "\n")
        self._count += len(spans)
        return len(spans)

    @property
    def total_exported(self) -> int:
        """Total spans exported so far."""
        return self._count


class ConsoleExporter(SpanExporter):
    """Prints spans to a text stream (stdout by default).

    Attributes:
        stream: Output stream.
        verbose: If True, print full span details.
    """

    def __init__(self, stream: TextIO = sys.stdout, verbose: bool = False) -> None:
        self.stream = stream
        self.verbose = verbose

    def export(self, spans: list[Span]) -> int:
        """Print spans to the console.

        Args:
            spans: Spans to print.

        Returns:
            Number of spans printed.
        """
        for s in spans:
            if self.verbose:
                self.stream.write(json.dumps(s.to_dict(), indent=2) + "\n")
            else:
                dur = f"{s.duration_ms:.1f}ms" if s.end_time else "active"
                self.stream.write(
                    f"[{s.service_name}] {s.operation_name} "
                    f"({dur}) trace={s.trace_id[:8]}… span={s.span_id[:8]}…\n"
                )
        return len(spans)


class CompositeExporter(SpanExporter):
    """Fan-out exporter that sends spans to multiple backends.

    Attributes:
        exporters: List of downstream exporters.
    """

    def __init__(self, exporters: list[SpanExporter]) -> None:
        self.exporters = exporters

    def export(self, spans: list[Span]) -> int:
        """Export to all backends, returning the minimum success count.

        Args:
            spans: Spans to export.

        Returns:
            Minimum number of spans exported across all backends.
        """
        if not self.exporters:
            return 0
        counts = [e.export(spans) for e in self.exporters]
        return min(counts)

    def shutdown(self) -> None:
        """Shut down all downstream exporters."""
        for e in self.exporters:
            e.shutdown()


# -----------------------------------------------------------------------
# __main__ demo
# -----------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile
    from ..trace.trace import generate_trace

    print("=== Span Exporter Demo ===\n")

    spans = generate_trace(n_services=4, max_depth=3)

    # In-memory exporter
    mem = InMemoryExporter()
    n = mem.export(spans)
    print(f"InMemoryExporter: stored {n} spans, "
          f"{len(mem.trace_ids)} trace(s)")

    # Console exporter
    print("\nConsoleExporter output:")
    console = ConsoleExporter()
    console.export(spans[:3])

    # JSON file exporter
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        path = Path(f.name)
    jf = JsonFileExporter(path=path)
    jf.export(spans)
    print(f"\nJsonFileExporter: wrote {jf.total_exported} spans to {path}")
    print(f"  File size: {path.stat().st_size} bytes")
    path.unlink()

    # Composite exporter
    mem2 = InMemoryExporter()
    composite = CompositeExporter([mem2, ConsoleExporter(verbose=False)])
    print("\nCompositeExporter (memory + console):")
    composite.export(spans[:2])
    print(f"  Memory backend has {len(mem2.spans)} spans")
