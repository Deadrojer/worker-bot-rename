"""
helper/transfer_metrics.py
════════════════════════════════════════════════════════════════════════════
Lightweight per-job transfer metrics.

Usage
─────
    from helper.transfer_metrics import JobMetrics

    m = JobMetrics(job_id)
    m.start_download(expected_bytes)
    ...
    m.end_download(actual_bytes)
    m.start_metadata()
    m.end_metadata()
    m.start_upload(expected_bytes)
    ...
    m.end_upload(actual_bytes)
    m.record_floodwait(seconds)
    m.log_summary()          # writes TRANSFER SUMMARY to logger

No external dependencies.  All arithmetic is pure Python.
"""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


def _hb(n: int) -> str:
    """Human-readable bytes."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _speed(nbytes: float, seconds: float) -> str:
    if seconds <= 0:
        return "N/A"
    return f"{_hb(int(nbytes / seconds))}/s"


@dataclass
class _Phase:
    label:    str
    started:  float = 0.0
    ended:    float = 0.0
    bytes_in: int   = 0
    bytes_out: int  = 0
    retries:  int   = 0
    extra:    float = 0.0   # e.g. floodwait_seconds

    @property
    def duration(self) -> float:
        return max(0.0, self.ended - self.started) if self.ended else 0.0

    def summary(self) -> list[str]:
        lines = [f"{self.label}:"]
        dur = self.duration
        if self.bytes_out:
            lines.append(f"  Size:     {_hb(self.bytes_out)}")
        elif self.bytes_in:
            lines.append(f"  Size:     {_hb(self.bytes_in)}")
        if dur:
            lines.append(f"  Time:     {dur:.1f}s")
        if self.bytes_out and dur:
            lines.append(f"  Speed:    {_speed(self.bytes_out, dur)}")
        elif self.bytes_in and dur:
            lines.append(f"  Speed:    {_speed(self.bytes_in, dur)}")
        if self.retries:
            lines.append(f"  Retries:  {self.retries}")
        if self.extra:
            lines.append(f"  FloodWait: {self.extra:.0f}s")
        return lines


class JobMetrics:
    """Collects timing and bandwidth data for one rename job."""

    def __init__(self, job_id: str):
        self.job_id    = job_id
        self._created  = time.time()
        self._download = _Phase("Download")
        self._metadata = _Phase("Metadata")
        self._upload   = _Phase("Upload")
        self._mediainfo = _Phase("MediaInfo")
        self._dump     = _Phase("Dump")

    # ── Download ──────────────────────────────────────────────────────────────

    def start_download(self, expected_bytes: int = 0) -> None:
        self._download.started  = time.time()
        self._download.bytes_in = expected_bytes

    def end_download(self, actual_bytes: int, retries: int = 0) -> None:
        self._download.ended    = time.time()
        self._download.bytes_out = actual_bytes
        self._download.retries  = retries

    # ── Metadata ──────────────────────────────────────────────────────────────

    def start_metadata(self) -> None:
        self._metadata.started = time.time()

    def end_metadata(self) -> None:
        self._metadata.ended = time.time()

    # ── Upload ────────────────────────────────────────────────────────────────

    def start_upload(self, expected_bytes: int = 0) -> None:
        self._upload.started  = time.time()
        self._upload.bytes_in = expected_bytes

    def end_upload(self, actual_bytes: int, retries: int = 0) -> None:
        self._upload.ended    = time.time()
        self._upload.bytes_out = actual_bytes
        self._upload.retries  = retries

    def record_floodwait(self, seconds: float) -> None:
        self._upload.extra += seconds
        self._upload.retries += 1

    # ── MediaInfo ─────────────────────────────────────────────────────────────

    def start_mediainfo(self) -> None:
        self._mediainfo.started = time.time()

    def end_mediainfo(self) -> None:
        self._mediainfo.ended = time.time()

    # ── Dump ──────────────────────────────────────────────────────────────────

    def start_dump(self) -> None:
        self._dump.started = time.time()

    def end_dump(self) -> None:
        self._dump.ended = time.time()

    # ── Summary ───────────────────────────────────────────────────────────────

    def log_summary(self) -> None:
        total = time.time() - self._created
        sep   = "━" * 38
        lines = [
            "",
            "TRANSFER SUMMARY",
            sep,
            f"Job: {self.job_id}",
            "",
        ]
        for phase in (self._download, self._metadata, self._upload,
                      self._mediainfo, self._dump):
            if phase.started:
                lines.extend(phase.summary())
                lines.append("")
        lines += [f"Total:    {total:.1f}s", sep]
        logger.info("\n".join(lines))
