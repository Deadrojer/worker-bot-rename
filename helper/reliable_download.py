"""
helper/reliable_download.py
════════════════════════════════════════════════════════════════════════════
Reliable, validated Pyrogram download with:

  • Exponential back-off on Telegram 503 / timeout errors
  • 0-byte download treated as failure (never passed downstream)
  • Size validation against Telegram's reported file_size
  • Partial-file cleanup before each retry attempt
  • Memory-efficient streaming (no file.read() into RAM)
  • Progress callback throttled by caller (no duplicate throttle here)

Usage (drop-in for bot.download_media)
────────────────────────────────────────
    from helper.reliable_download import download_with_retry

    file_path = await download_with_retry(
        client       = bot,
        message      = file_message,   # or media object
        file_name    = "/path/to/dest.mkv",
        expected_size= media.file_size,   # 0 = skip size check
        progress     = _my_progress_cb,   # optional
        progress_args= (...,),            # optional
        max_attempts = 4,
    )
    # Returns path on success, raises DownloadFailedError on all-retry failure.

Exceptions
──────────
    DownloadFailedError(message, last_exception, attempt_count)
    — Raised when all retry attempts are exhausted.  The job must stop
      cleanly at this point and must NOT continue to rename/metadata/dump.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# ── Retry timing (seconds) ────────────────────────────────────────────────────
_BACKOFF_BASE   = 5      # first retry wait
_BACKOFF_MAX    = 60     # cap per retry
_JITTER_RANGE   = 3      # ± jitter added to each wait

# ── Size tolerance ────────────────────────────────────────────────────────────
# Pyrogram may return slightly different byte counts (buffering, network
# chunking). Accept anything ≥ 98 % of the reported Telegram file_size.
_SIZE_TOLERANCE = 0.98

# ── Transient Telegram errors that warrant a retry ───────────────────────────
# These are the error types known to occur on temporary server overload.
_RETRYABLE_ERRORS = (
    "Timeout",          # pyrogram.errors.Timeout (503)
    "FloodWait",        # flood-wait; we honour the wait seconds
    "InternalServerError",
    "ServerError",
    "ServiceUnavailable",
    "BadRequest",       # occasionally transient on large files
    "ConnectionError",
    "TimeoutError",
    "ConnectionResetError",
    "OSError",
)


class DownloadFailedError(Exception):
    """Raised when all download attempts are exhausted."""

    def __init__(self, msg: str, last_exc: Optional[Exception] = None, attempts: int = 0):
        super().__init__(msg)
        self.last_exc  = last_exc
        self.attempts  = attempts


# ── Helpers ───────────────────────────────────────────────────────────────────

def _remove_partial(path: str) -> None:
    """Silently remove a partial/invalid download file."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
            logger.debug("[dl] Removed partial file: %s", path)
    except OSError as e:
        logger.debug("[dl] Could not remove partial file %s: %s", path, e)


def _is_retryable(exc: Exception) -> bool:
    """Return True if the exception is a known transient Telegram error."""
    name = type(exc).__name__
    for pat in _RETRYABLE_ERRORS:
        if pat.lower() in name.lower():
            return True
    # Also match on the string representation (covers pyrogram nested errors)
    err_str = str(exc).lower()
    for kw in ("timeout", "503", "flood", "internal", "server error",
                "connection reset", "connection refused", "broken pipe"):
        if kw in err_str:
            return True
    return False


def _backoff(attempt: int) -> float:
    """Return seconds to wait before attempt N (1-indexed), with jitter."""
    wait = min(_BACKOFF_BASE * (2 ** (attempt - 1)), _BACKOFF_MAX)
    jitter = random.uniform(-_JITTER_RANGE, _JITTER_RANGE)
    return max(1.0, wait + jitter)


# ── Main public API ───────────────────────────────────────────────────────────

async def download_with_retry(
    client,
    message,
    file_name: str,
    expected_size: int = 0,
    progress: Optional[Callable] = None,
    progress_args: tuple = (),
    max_attempts: int = 4,
    status_msg=None,
    job_id: str = "",
) -> str:
    """
    Download ``message`` to ``file_name`` with retry/backoff.

    Parameters
    ──────────
    client        : Pyrogram Client (bot or userbot).
    message       : Pyrogram Message **or** a media object (document/video/audio).
                    Passed directly to client.download_media().
    file_name     : Absolute destination path.
    expected_size : Telegram-reported file_size (bytes). 0 = skip size check.
    progress      : Optional Pyrogram-compatible progress callback.
    progress_args : Extra positional args passed after (current, total) to progress.
    max_attempts  : Total download attempts (1 = no retry).
    status_msg    : Optional status message object — updated on retry (best-effort).
    job_id        : Job identifier for log messages.

    Returns
    ───────
    str — absolute path to the successfully downloaded file.

    Raises
    ──────
    asyncio.CancelledError — propagated immediately (never retried).
    DownloadFailedError    — all attempts failed.
    """
    label    = f"[dl job={job_id}]" if job_id else "[dl]"
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        # ── Clean any previous partial file before each attempt ───────────────
        _remove_partial(file_name)

        logger.info(
            "%s Attempt %d/%d  expected=%s  dest=%s",
            label, attempt, max_attempts,
            f"{expected_size // 1024 // 1024} MB" if expected_size else "unknown",
            file_name,
        )

        try:
            result = await client.download_media(
                message=message,
                file_name=file_name,
                progress=progress,
                progress_args=progress_args,
            )
        except asyncio.CancelledError:
            # Never retry a cancellation
            _remove_partial(file_name)
            raise
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "%s Attempt %d EXCEPTION  type=%s  msg=%s",
                label, attempt, type(exc).__name__, exc,
            )
            _remove_partial(file_name)

            if not _is_retryable(exc) or attempt >= max_attempts:
                break

            # FloodWait carries the required wait in seconds
            flood_wait = getattr(exc, "value", 0) or 0
            wait_sec   = max(float(flood_wait) + 2, _backoff(attempt))
            logger.info("%s Retrying in %.1f s…", label, wait_sec)
            await _notify_retry(status_msg, job_id, attempt, max_attempts, exc, wait_sec)
            await asyncio.sleep(wait_sec)
            continue

        # ── Validate the result ───────────────────────────────────────────────
        dl_path = result or file_name

        # Must exist
        if not dl_path or not os.path.exists(dl_path):
            logger.warning("%s Attempt %d result path does not exist: %r", label, attempt, dl_path)
            _remove_partial(dl_path or file_name)
            last_exc = DownloadFailedError("Downloaded path does not exist", attempts=attempt)
            if attempt >= max_attempts:
                break
            wait_sec = _backoff(attempt)
            await _notify_retry(status_msg, job_id, attempt, max_attempts, last_exc, wait_sec)
            await asyncio.sleep(wait_sec)
            continue

        dl_size = os.path.getsize(dl_path)

        # Must be non-zero
        if dl_size == 0:
            logger.warning(
                "%s Attempt %d — Download finished size=0 B (expected %s) — FAIL",
                label, attempt,
                f"{expected_size // 1024 // 1024} MB" if expected_size else "unknown",
            )
            _remove_partial(dl_path)
            last_exc = DownloadFailedError(
                f"Downloaded 0 B (expected {expected_size} B)", attempts=attempt
            )
            if attempt >= max_attempts:
                break
            wait_sec = _backoff(attempt)
            await _notify_retry(status_msg, job_id, attempt, max_attempts, last_exc, wait_sec)
            await asyncio.sleep(wait_sec)
            continue

        # Must meet size tolerance when expected_size is known
        if expected_size > 0 and dl_size < int(expected_size * _SIZE_TOLERANCE):
            logger.warning(
                "%s Attempt %d — Size mismatch: expected≥%d B, got %d B",
                label, attempt, int(expected_size * _SIZE_TOLERANCE), dl_size,
            )
            _remove_partial(dl_path)
            last_exc = DownloadFailedError(
                f"Incomplete download: expected≥{expected_size} B, got {dl_size} B",
                attempts=attempt,
            )
            if attempt >= max_attempts:
                break
            wait_sec = _backoff(attempt)
            await _notify_retry(status_msg, job_id, attempt, max_attempts, last_exc, wait_sec)
            await asyncio.sleep(wait_sec)
            continue

        # ── SUCCESS ───────────────────────────────────────────────────────────
        logger.info(
            "%s Attempt %d SUCCESS  size=%d B  path=%s",
            label, attempt, dl_size, dl_path,
        )
        return dl_path

    # ── All attempts exhausted ────────────────────────────────────────────────
    _remove_partial(file_name)
    reason = str(last_exc) if last_exc else "Unknown error"
    raise DownloadFailedError(
        f"Download failed after {max_attempts} attempt(s): {reason}",
        last_exc=last_exc,
        attempts=max_attempts,
    )


async def _notify_retry(status_msg, job_id: str, attempt: int, max_attempts: int,
                         exc: Exception, wait_sec: float) -> None:
    """Best-effort status message update before a retry wait."""
    if not status_msg:
        return
    try:
        reason = type(exc).__name__
        await status_msg.edit(
            f"╭━━━〔 ⚠️ DOWNLOAD RETRY 〕━━━╮\n"
            f"┃  🆔  <code>{job_id}</code>\n"
            f"┃  ⚠️  Attempt {attempt}/{max_attempts} failed\n"
            f"┃  📡  <code>{reason}</code>\n"
            f"┃  ⏳  Retrying in {wait_sec:.0f} s…\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    except Exception:
        pass
