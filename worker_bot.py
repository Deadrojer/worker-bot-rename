"""
worker/worker_bot.py
══════════════════════════════════════════════════════════════════════════════
Worker Bot entry point — Bot-only edition (no String Session).

The worker communicates with the Manager entirely via its Bot token.
The bot must be added as a member of the Worker Control Group so it can
send and receive messages there.

Startup sequence
────────────────
  1. Connect Worker Bot (Pyrogram Client with BOT_TOKEN)
  2. Instantiate JobPipeline (download/process/upload engine)
  3. Instantiate WorkerProtocolHandler (sends REGISTER/HEARTBEAT/ACK/STATE/
     RESULT/FAILED via bot; receives TASK from the group via bot)
  4. Start protocol handler (sends REGISTER, starts heartbeat loop)
  5. Start aiohttp health-check server (Koyeb/Render keep-alive)
  6. Block until SIGINT/SIGTERM

Shutdown sequence
─────────────────
  1. Stop WorkerProtocolHandler heartbeat loop
  2. Wait up to SHUTDOWN_GRACE_SECONDS for in-flight jobs to finish
  3. Stop Worker Bot client
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import signal

import pyrogram.utils
from aiohttp import web
from pyrogram import Client

from config import Config

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for _noisy in ("pyrogram", "aiohttp"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(f"worker.{Config.WORKER_ID}")

# ── Pyrogram large-channel ID patch ──────────────────────────────────────────
pyrogram.utils.MIN_CHAT_ID    = -999_999_999_999
pyrogram.utils.MIN_CHANNEL_ID = -1_009_999_999_999

# ── Global references ─────────────────────────────────────────────────────────
_bot_client:   Client | None = None
_proto_handler               = None
_pipeline                    = None
_active_tasks: set[asyncio.Task] = set()


# ──────────────────────────────────────────────────────────────────────────────
# Task dispatcher  (injected into WorkerProtocolHandler as on_task callback)
# ──────────────────────────────────────────────────────────────────────────────

async def _on_task(task: dict) -> None:
    """
    Called by WorkerProtocolHandler when a TASK addressed to this worker arrives.
    Runs the full job pipeline.
    """
    job_id = task.get("job_id", "?")
    t = asyncio.create_task(_pipeline.run(task), name=f"job_{job_id}")
    _active_tasks.add(t)
    t.add_done_callback(_active_tasks.discard)
    await t   # protocol_handler._run_task wraps this — we await here so
              # decrement_active() fires AFTER the job actually finishes.


# ──────────────────────────────────────────────────────────────────────────────
# Health server
# ──────────────────────────────────────────────────────────────────────────────

async def _web_server() -> web.Application:
    async def health(_):
        active = len(_active_tasks)
        cap    = Config.WORKER_CONCURRENCY
        return web.Response(
            text=f"Worker {Config.WORKER_ID} OK  active={active}/{cap}"
        )

    app = web.Application()
    app.router.add_get("/",       health)
    app.router.add_get("/health", health)
    return app


# ──────────────────────────────────────────────────────────────────────────────
# Startup
# ──────────────────────────────────────────────────────────────────────────────

async def _startup() -> None:
    global _bot_client, _proto_handler, _pipeline

    # ── 1. Worker Bot ─────────────────────────────────────────────────────────
    _bot_client = Client(
        name           = f"worker_bot_{Config.WORKER_ID}",
        api_id         = Config.API_ID,
        api_hash       = Config.API_HASH,
        bot_token      = Config.BOT_TOKEN,
        workers        = Config.WORKER_CONCURRENCY + 1,
        sleep_threshold= 30,
        in_memory      = True,
    )
    await _bot_client.start()
    me = await _bot_client.get_me()
    logger.info("[worker] Bot started: %s (@%s)", me.first_name, me.username)

    # ── 2. Wire pipeline + protocol handler ───────────────────────────────────
    # Chicken-and-egg: pipeline needs proto_handler's send_* methods,
    # proto_handler needs on_task which runs pipeline.run().
    # Solution: create proto_handler with a stub, wire pipeline, swap stub.

    from helper.pipeline import JobPipeline
    from helper.protocol_handler import WorkerProtocolHandler as _WPH

    _proto_handler = _WPH(
        bot_client         = _bot_client,
        worker_id          = Config.WORKER_ID,
        control_group_id   = Config.WORKER_CONTROL_GROUP_ID,
        capacity           = Config.WORKER_CONCURRENCY,
        heartbeat_interval = Config.HEARTBEAT_INTERVAL,
        on_task            = _task_stub,   # replaced below
    )

    _pipeline = JobPipeline(
        bot_client  = _bot_client,
        send_state  = _proto_handler.send_state,
        send_result = _proto_handler.send_result,
        send_failed = _proto_handler.send_failed,
    )

    # Replace stub with real dispatcher
    _proto_handler._on_task = _on_task

    # ── 3. Start protocol handler ─────────────────────────────────────────────
    await _proto_handler.start()

    # ── 4. Health server ──────────────────────────────────────────────────────
    runner = web.AppRunner(await _web_server())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", Config.PORT).start()
    logger.info("[worker] Health server on port %d", Config.PORT)

    logger.info(
        "[worker] Ready — id=%s capacity=%d group=%s",
        Config.WORKER_ID, Config.WORKER_CONCURRENCY, Config.WORKER_CONTROL_GROUP_ID,
    )


async def _task_stub(task: dict) -> None:
    """Placeholder — replaced with _on_task after pipeline is built."""
    logger.warning("[worker] _task_stub called — pipeline not ready yet")


# ──────────────────────────────────────────────────────────────────────────────
# Shutdown
# ──────────────────────────────────────────────────────────────────────────────

async def _shutdown() -> None:
    logger.info("[worker] Initiating shutdown — id=%s", Config.WORKER_ID)

    # 1. Stop accepting new tasks
    if _proto_handler:
        await _proto_handler.stop()

    # 2. Wait for in-flight jobs (up to grace period)
    if _active_tasks:
        logger.info(
            "[worker] Waiting up to %ds for %d active job(s)…",
            Config.SHUTDOWN_GRACE_SECONDS, len(_active_tasks),
        )
        try:
            await asyncio.wait_for(
                asyncio.gather(*_active_tasks, return_exceptions=True),
                timeout=Config.SHUTDOWN_GRACE_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "[worker] Grace period expired — cancelling %d job(s)",
                len(_active_tasks),
            )
            for t in _active_tasks:
                t.cancel()

    # 3. Stop bot client
    if _bot_client:
        try:
            await _bot_client.stop()
        except Exception as exc:
            logger.debug("[worker] Bot stop error: %s", exc)

    logger.info("[worker] Shutdown complete — id=%s", Config.WORKER_ID)


def _handle_signal(sig, loop: asyncio.AbstractEventLoop) -> None:
    logger.info("[worker] Signal %s received", sig.name)
    loop.create_task(_shutdown())


async def _main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig, loop)

    await _startup()

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass

    await _shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
