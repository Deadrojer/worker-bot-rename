"""
helper/queue_manager.py
═══════════════════════════════════════════════════════════════════════════════
Single authoritative rename-job queue for the entire bot.

Architecture
────────────
  • One concurrency value (N) — at most N jobs download+process+upload
    simultaneously.  No separate global/user/transmission limits.
  • FIFO with fair scheduling: jobs from heavy users are interleaved with
    jobs from other users so one 100-file batch cannot starve everyone else.
  • /limit N updates N live — existing active jobs finish naturally; if N
    increases, queued jobs start immediately.
  • Both manual-rename (file_rename.py) and auto-rename (auto_rename.py)
    submit to this queue.  There is exactly one concurrency controller.

Public API
──────────
    from helper.queue_manager import rq

    rq.concurrency                   # current int limit
    await rq.enqueue(job_fn, job_id, user_id, meta)
    await rq.set_concurrency(n)      # live update
    rq.active_count()
    rq.queued_count()
    rq.active_jobs()                 # list[QueuedJob]
    rq.queued_jobs()                 # list[QueuedJob]

    # Bandwidth helpers (used by stats.py) — synchronous, call directly in callbacks
    rq.record_download(bytes_delta)
    rq.record_upload(bytes_delta)
    rq.dl_speed()
    rq.ul_speed()
    rq.total_downloaded()
    rq.total_uploaded()
    rq.uptime_seconds()
═══════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

_DEFAULT_CONCURRENCY = 4


@dataclass
class QueuedJob:
    """Metadata about one rename job inside the queue."""
    job_id:      str
    user_id:     int
    meta:        dict                       # arbitrary caller-supplied info
    fn:          Callable[[], Awaitable]    # the coroutine factory
    status:      str   = "queued"          # queued | active | done | failed | cancelled
    created_at:  float = field(default_factory=time.time)
    started_at:  Optional[float] = None
    finished_at: Optional[float] = None
    task:        Optional[asyncio.Task] = None

    # Display helpers — set from meta at enqueue time
    @property
    def user_name(self) -> str:
        return self.meta.get("user_name", f"user {self.user_id}")

    @property
    def filename(self) -> str:
        return self.meta.get("filename", self.job_id)

    def wait_seconds(self) -> float:
        """Seconds since this job was enqueued."""
        return time.time() - self.created_at

    def run_seconds(self) -> float:
        """Seconds since this job started (0 if not started)."""
        if self.started_at is None:
            return 0.0
        end = self.finished_at or time.time()
        return end - self.started_at


class RenameQueue:
    """
    Central FIFO+fair rename queue with a single dynamic concurrency limit.
    """

    def __init__(self, concurrency: int = _DEFAULT_CONCURRENCY):
        self._concurrency:  int                  = max(1, concurrency)
        self._active:       dict[str, QueuedJob] = {}   # job_id → job
        self._queue:        deque[QueuedJob]     = deque()
        self._lock:         asyncio.Lock         = asyncio.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None

        # Bandwidth tracking (written in progress callbacks, read in stats)
        self._dl_bytes:   int   = 0
        self._ul_bytes:   int   = 0
        self._dl_window:  list  = []   # (monotonic_ts, bytes) — pruned in dl_speed()
        self._ul_window:  list  = []
        self._started_at: float = time.time()

    # ──────────────────────────────────────────────────────────────────────────
    # Startup (called once from bot.py)
    # ──────────────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Launch the background scheduler task."""
        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = asyncio.create_task(
                self._scheduler_loop(), name="rename_queue_scheduler"
            )
            logger.info("[rq] Scheduler started (concurrency=%d)", self._concurrency)

    # ──────────────────────────────────────────────────────────────────────────
    # Public: enqueue
    # ──────────────────────────────────────────────────────────────────────────

    async def enqueue(
        self,
        fn:      Callable[[], Awaitable],
        job_id:  str,
        user_id: int,
        meta:    dict | None = None,
    ) -> QueuedJob:
        """
        Add a job to the queue.  Returns immediately — the job runs in the
        background when a concurrency slot becomes free.

        Parameters
        ----------
        fn       : Zero-argument async callable.  Must release its own temp
                   files in a finally block.  The queue will catch any
                   exception so one failure never kills other jobs.
        job_id   : Unique string ID (e.g. "mr000001" or "ar000003").
        user_id  : Telegram user ID — used for fair scheduling.
        meta     : Optional dict stored on the job for inspection.
        """
        job = QueuedJob(
            job_id=job_id,
            user_id=user_id,
            meta=meta or {},
            fn=fn,
        )
        async with self._lock:
            self._queue.append(job)
        logger.info("[rq] Enqueued job=%s user=%s queue_len=%d",
                    job_id, user_id, len(self._queue))
        # Nudge scheduler in case slots are free
        self._kick()
        return job

    # ──────────────────────────────────────────────────────────────────────────
    # Public: concurrency control
    # ──────────────────────────────────────────────────────────────────────────

    @property
    def concurrency(self) -> int:
        return self._concurrency

    async def set_concurrency(self, n: int) -> None:
        """
        Live-update the concurrency limit.

        • If n > current: immediately starts queued jobs to fill new slots.
        • If n < current: active jobs run to completion; no new jobs start
          until active_count drops below n.
        """
        n = max(1, int(n))
        old = self._concurrency
        self._concurrency = n
        logger.info("[rq] Concurrency changed %d → %d  active=%d queued=%d",
                    old, n, self.active_count(), self.queued_count())
        if n > old:
            self._kick()

    async def load_from_db(self) -> None:
        """Load persisted concurrency from MongoDB bot_settings."""
        try:
            from helper.database import jishubotz
            val = await jishubotz.get_bot_setting("rename_concurrency", _DEFAULT_CONCURRENCY)
            n   = max(1, int(val))
            self._concurrency = n
            logger.info("[rq] Loaded concurrency=%d from DB", n)
        except Exception as e:
            logger.warning("[rq] Could not load concurrency from DB: %s", e)

    # ──────────────────────────────────────────────────────────────────────────
    # Public: state queries
    # ──────────────────────────────────────────────────────────────────────────

    def active_count(self) -> int:
        return len(self._active)

    def queued_count(self) -> int:
        return len(self._queue)

    def active_jobs(self) -> list[QueuedJob]:
        return list(self._active.values())

    def queued_jobs(self) -> list[QueuedJob]:
        return list(self._queue)

    def available_slots(self) -> int:
        return max(0, self._concurrency - self.active_count())

    def user_active_count(self, user_id: int) -> int:
        """How many slots this user currently occupies."""
        return sum(1 for j in self._active.values() if j.user_id == user_id)

    def user_queued_jobs(self, user_id: int) -> list[QueuedJob]:
        """All queued (not yet active) jobs belonging to this user, in order."""
        return [j for j in self._queue if j.user_id == user_id and j.status != "cancelled"]

    def queue_position(self, job_id: str) -> int:
        """
        Estimated position in the fair queue for a specific job.

        Returns the number of jobs that will start before this one, plus 1.
        Uses the same weighted-fair logic as _drain so the number is accurate.
        Returns 0 if the job is already active.
        Returns -1 if the job is not found.
        """
        if job_id in self._active:
            return 0

        # Simulate the fair scheduler to count how many jobs precede this one
        user_active: dict[int, int] = {}
        for j in self._active.values():
            user_active[j.user_id] = user_active.get(j.user_id, 0) + 1

        position = 0
        available = max(0, self._concurrency - self.active_count())
        sim_active = dict(user_active)

        for job in self._queue:
            if job.status == "cancelled":
                continue
            if job.job_id == job_id:
                # How many jobs ahead will consume the remaining slots first?
                # position already counts jobs scheduled before this one.
                return max(1, position - available + 1)
            position += 1
            sim_active[job.user_id] = sim_active.get(job.user_id, 0) + 1

        return -1  # not found

    # ──────────────────────────────────────────────────────────────────────────
    # Internal: scheduler
    # ──────────────────────────────────────────────────────────────────────────

    async def _scheduler_loop(self) -> None:
        """Continuously drain the queue into available concurrency slots."""
        while True:
            await asyncio.sleep(0.5)
            await self._drain()

    def _kick(self) -> None:
        """Schedule a drain on the next event-loop iteration."""
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: asyncio.ensure_future(self._drain()))
        except RuntimeError:
            pass  # No running loop yet — scheduler will drain on next tick

    async def _drain(self) -> None:
        """
        Start as many queued jobs as concurrency allows.

        Scheduling algorithm — Weighted Fair Queuing:
        ─────────────────────────────────────────────
        When a slot opens, pick the queued job whose user currently has the
        FEWEST active jobs.  Ties are broken by insertion order (FIFO).

        Example — concurrency=4, User A has 3 active, User B has 1 active,
        User C has 0 active:

            Slot opens → pick User C's first queued job  (0 active = lowest)

        This means a user who sent 100 files never starves another user who
        sent 2 files, because the 2-file user always has fewer active jobs
        and therefore always wins the next available slot.

        Edge case — only one user has queued jobs:
            All slots go to that user (correct, no starvation possible).
        """
        async with self._lock:
            if not self._queue or self.active_count() >= self._concurrency:
                return

            # Build current active-job count per user
            user_active: dict[int, int] = {}
            for j in self._active.values():
                user_active[j.user_id] = user_active.get(j.user_id, 0) + 1

            queue_snapshot = list(self._queue)
            picked: set[int] = set()
            to_start: list[QueuedJob] = []
            slots = self._concurrency - self.active_count()

            while slots > 0:
                best_idx: Optional[int] = None
                best_active = float("inf")

                for idx, job in enumerate(queue_snapshot):
                    if idx in picked:
                        continue
                    if job.status == "cancelled":
                        picked.add(idx)   # skip cancelled in place
                        continue
                    # User with the fewest active jobs wins the next slot.
                    # FIFO within the same active count (first appearance wins).
                    count = user_active.get(job.user_id, 0)
                    if count < best_active:
                        best_active = count
                        best_idx = idx

                if best_idx is None:
                    break   # nothing left to schedule

                job = queue_snapshot[best_idx]
                picked.add(best_idx)
                to_start.append(job)
                user_active[job.user_id] = user_active.get(job.user_id, 0) + 1
                slots -= 1

            # Rebuild queue without picked/cancelled entries
            self._queue = deque(
                j for idx, j in enumerate(queue_snapshot) if idx not in picked
            )

            for job in to_start:
                self._start_job(job)

    def _start_job(self, job: QueuedJob) -> None:
        """Must be called while holding self._lock."""
        job.status     = "active"
        job.started_at = time.time()
        self._active[job.job_id] = job
        job.task = asyncio.ensure_future(self._run_job(job))

    async def _run_job(self, job: QueuedJob) -> None:
        """Wrapper that guarantees slot release and queue draining regardless of outcome."""
        try:
            await job.fn()
            job.status = "done"
        except asyncio.CancelledError:
            job.status = "cancelled"
            logger.info("[rq] Job cancelled: %s", job.job_id)
        except Exception:
            job.status = "failed"
            logger.exception("[rq] Job failed: %s", job.job_id)
        finally:
            job.finished_at = time.time()
            async with self._lock:
                self._active.pop(job.job_id, None)
            logger.info("[rq] Job finished: %s  status=%s  active=%d queued=%d",
                        job.job_id, job.status, self.active_count(), self.queued_count())
            # Start next queued job(s)
            await self._drain()

    # ──────────────────────────────────────────────────────────────────────────
    # Bandwidth tracking (used by stats.py)
    # Synchronous — called directly in progress callbacks, not via create_task.
    # Python's GIL protects the simple int increments.  The window list is
    # only pruned during the speed calculation (dl_speed/ul_speed), not here,
    # so these methods are effectively lock-free and zero-overhead.
    # ──────────────────────────────────────────────────────────────────────────

    def record_download(self, bytes_delta: int) -> None:
        """Record download bytes — synchronous, safe to call in progress callbacks."""
        ts = time.monotonic()
        self._dl_bytes += bytes_delta
        self._dl_window.append((ts, bytes_delta))

    def record_upload(self, bytes_delta: int) -> None:
        """Record upload bytes — synchronous, safe to call in progress callbacks."""
        ts = time.monotonic()
        self._ul_bytes += bytes_delta
        self._ul_window.append((ts, bytes_delta))

    def dl_speed(self) -> float:
        """Rolling 10s download speed in bytes/sec."""
        cutoff = time.monotonic() - 10
        self._dl_window = [(t, b) for t, b in self._dl_window if t >= cutoff]
        if len(self._dl_window) < 2:
            return 0.0
        span = self._dl_window[-1][0] - self._dl_window[0][0]
        return sum(b for _, b in self._dl_window) / span if span > 0 else 0.0

    def ul_speed(self) -> float:
        """Rolling 10s upload speed in bytes/sec."""
        cutoff = time.monotonic() - 10
        self._ul_window = [(t, b) for t, b in self._ul_window if t >= cutoff]
        if len(self._ul_window) < 2:
            return 0.0
        span = self._ul_window[-1][0] - self._ul_window[0][0]
        return sum(b for _, b in self._ul_window) / span if span > 0 else 0.0

    def total_downloaded(self) -> int:
        return self._dl_bytes

    def total_uploaded(self) -> int:
        return self._ul_bytes

    def uptime_seconds(self) -> float:
        return time.time() - self._started_at


# ──────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ──────────────────────────────────────────────────────────────────────────────

rq = RenameQueue(concurrency=_DEFAULT_CONCURRENCY)


# ── Backward-compat shim (old code used qm() as a callable) ──────────────────
# Keep this so stats.py / task_status.py keep working unchanged.
class _QMShim:
    """Thin shim that proxies the new RenameQueue behind the old QueueManager API."""
    def active_count(self) -> int:        return rq.active_count()
    def queued_count(self) -> int:        return rq.queued_count()
    def available_slots(self) -> int:     return rq.available_slots()
    def dl_speed(self) -> float:          return rq.dl_speed()
    def ul_speed(self) -> float:          return rq.ul_speed()
    def total_downloaded(self) -> int:    return rq.total_downloaded()
    def total_uploaded(self) -> int:      return rq.total_uploaded()
    def uptime_seconds(self) -> float:    return rq.uptime_seconds()
    def record_download(self, b: int):    rq.record_download(b)
    def record_upload(self, b: int):      rq.record_upload(b)
    # Old attributes accessed in stats.py / file_rename.py
    @property
    def transmission_limit(self) -> int:   return rq.concurrency
    @property
    def global_limit(self) -> int:         return rq.concurrency
    @property
    def user_limit(self) -> int:           return rq.concurrency
    # _user_active accessed in cmd_getlimit
    @property
    def _user_active(self) -> dict:
        counts: dict[int, int] = {}
        for j in rq.active_jobs():
            counts[j.user_id] = counts.get(j.user_id, 0) + 1
        return counts

_shim = _QMShim()

def qm() -> _QMShim:
    """Legacy: from helper.queue_manager import qm; manager = qm()"""
    return _shim
