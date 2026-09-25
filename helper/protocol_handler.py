"""
worker/helper/protocol_handler.py
══════════════════════════════════════════════════════════════════════════════
Worker-side protocol handler — Bot-only edition.

All protocol I/O now goes through the Worker Bot token directly.
No String Session required on the worker side.

Flow
────
  Send  (REGISTER / HEARTBEAT / ACK / STATE / RESULT / FAILED)
        bot_client.send_message(control_group_id, proto_text)

  Receive (TASK from Manager String Session)
        bot_client MessageHandler on filters.all, gated on chat ID.
        Bots in a group receive ALL messages including from user accounts
        (the Manager's String Session), so this works without any session.

The Manager still uses its String Session to send TASKs — the worker bot
sees those messages normally because it's a member of the control group.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable, TYPE_CHECKING

from pyrogram.handlers import MessageHandler
from pyrogram import filters

from shared.protocol import (
    decode, is_proto,
    MSG_TASK,
    make_register, make_heartbeat, make_ack,
    make_state, make_result, make_failed,
    WORKER_ONLINE, WORKER_BUSY,
)

if TYPE_CHECKING:
    from pyrogram import Client

logger = logging.getLogger(__name__)


class WorkerProtocolHandler:

    def __init__(
        self,
        bot_client:         "Client",
        worker_id:          str,
        control_group_id:   int,
        capacity:           int,
        heartbeat_interval: int,
        on_task:            Callable[[dict], Awaitable[None]],
    ):
        self._bot          = bot_client
        self._worker_id    = worker_id
        self._group_id     = int(control_group_id)
        self._capacity     = capacity
        self._hb_interval  = heartbeat_interval
        self._on_task      = on_task

        self._active_jobs  = 0
        self._active_lock  = asyncio.Lock()
        self._hb_task: asyncio.Task | None = None
        self._running      = False

        # Bot receives all group messages including from the Manager's
        # String Session — no special filter needed beyond chat ID gate.
        self._bot.add_handler(
            MessageHandler(self._on_any_message, filters=filters.all)
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        bot_me = await self._bot.get_me()
        await self.send_register(
            bot_id   = bot_me.id,
            username = f"@{bot_me.username}" if bot_me.username else str(bot_me.id),
        )
        self._hb_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"hb_{self._worker_id}"
        )
        logger.info(
            "[worker] Protocol handler started — worker=%s bot=@%s capacity=%d",
            self._worker_id, bot_me.username, self._capacity,
        )

    async def stop(self) -> None:
        self._running = False
        if self._hb_task and not self._hb_task.done():
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass

    # ── Active job counter ────────────────────────────────────────────────────

    async def increment_active(self) -> None:
        async with self._active_lock:
            self._active_jobs += 1

    async def decrement_active(self) -> None:
        async with self._active_lock:
            self._active_jobs = max(0, self._active_jobs - 1)

    def active_count(self) -> int:
        return self._active_jobs

    def has_capacity(self) -> bool:
        return self._active_jobs < self._capacity

    # ── Outbound senders (all via bot token) ──────────────────────────────────

    async def send_register(self, bot_id: int, username: str) -> None:
        from config import Config
        await self._safe_send(make_register(
            worker_id = self._worker_id,
            bot_id    = bot_id,
            username  = username,
            capacity  = self._capacity,
            version   = Config.WORKER_VERSION,
        ), "REGISTER")

    async def send_ack(self, job_id: str) -> None:
        await self._safe_send(make_ack(self._worker_id, job_id), f"ACK {job_id}")

    async def send_state(self, job_id: str, state: str) -> None:
        await self._safe_send(make_state(self._worker_id, job_id, state), f"STATE {state}")

    async def send_result(
        self,
        job_id: str,
        output_chat_id: int,
        output_message_id: int,
        filename: str,
        original_filename: str,
        file_size: int,
        mediainfo_url: str | None = None,
    ) -> None:
        await self._safe_send(make_result(
            worker_id         = self._worker_id,
            job_id            = job_id,
            output_chat_id    = output_chat_id,
            output_message_id = output_message_id,
            filename          = filename,
            original_filename = original_filename,
            file_size         = file_size,
            mediainfo_url     = mediainfo_url,
        ), f"RESULT {job_id}")

    async def send_failed(self, job_id: str, reason: str) -> None:
        await self._safe_send(make_failed(self._worker_id, job_id, reason), f"FAILED {job_id}")

    async def _safe_send(self, text: str, label: str) -> None:
        try:
            await self._bot.send_message(self._group_id, text)
            logger.info("[worker] Sent %s", label)
        except Exception as exc:
            logger.error("[worker] Failed to send %s: %s", label, exc)

    # ── Heartbeat loop ────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                status = WORKER_BUSY if self._active_jobs > 0 else WORKER_ONLINE
                await self._safe_send(make_heartbeat(
                    worker_id   = self._worker_id,
                    bot_id      = 0,
                    active_jobs = self._active_jobs,
                    capacity    = self._capacity,
                    status      = status,
                ), "HEARTBEAT")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[worker] Heartbeat error: %s", exc)
            await asyncio.sleep(self._hb_interval)

    # ── Inbound: receive all group messages via bot ───────────────────────────

    async def _on_any_message(self, client, message) -> None:
        """
        Receives every update the bot sees in any chat.
        Gate 1: must be from the control group.
        Gate 2: must be a protocol message.
        Gate 3: must be a TASK addressed to this worker_id.
        """
        # Gate 1 — only control group
        chat_id = getattr(message.chat, "id", None)
        if chat_id != self._group_id:
            return

        # Gate 2 — protocol message check
        text = message.text or message.caption or ""
        if not is_proto(text):
            return

        msg = decode(text)
        if msg is None:
            return

        # Gate 3 — TASK addressed to this worker
        if msg.get("type") != MSG_TASK:
            return
        if msg.get("worker_id") != self._worker_id:
            return

        job_id = msg.get("job_id", "?")
        logger.info(
            "[worker] TASK received job=%s  active=%d/%d",
            job_id, self._active_jobs, self._capacity,
        )
        asyncio.create_task(self._run_task(msg), name=f"task_{job_id}")

    async def _run_task(self, task: dict) -> None:
        job_id = task.get("job_id", "?")
        await self.send_ack(job_id)
        await self.increment_active()
        try:
            await self._on_task(task)
        except asyncio.CancelledError:
            await self.send_failed(job_id, "Task cancelled")
        except Exception as exc:
            logger.exception("[worker] Task error job=%s: %s", job_id, exc)
            await self.send_failed(job_id, f"{type(exc).__name__}: {exc}")
        finally:
            await self.decrement_active()
