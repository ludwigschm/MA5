"""Quality control utilities for generating post-session reports."""

from .report import emit_latency_summary, emit_mapping_summary

__all__ = ["emit_mapping_summary", "emit_latency_summary"]
