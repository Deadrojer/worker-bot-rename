"""
helper/upload_manager.py
════════════════════════════════════════════════════════════════════════════
Centralized, FloodWait-aware Telegram upload helper.

Key properties
──────────────
• FloodWait on messages.SendMedia → wait required duration → retry.
  Job is NEVER marked done if upload has not confirmed success.
• Shared cooldown gate (_flood_cooldown_until) prevents retry storms when
  multiple concurrent jobs hit FloodWait at the same time.
• Non-FloodWait errors fail immediately (no blind retry → no duplicates).
• MediaInfo task is awaited (not fire-and-forget) before file cleanup so
  there is no race between ffprobe and file deletion.

Dump separation
───────────────
  dump_to_user_channel → caption = final filename ONLY
  dump_to_bot_channel  → caption = rich FILE INFO box + 📊 MediaInfo button

Both return a coroutine that should be awaited directly (not create_task)
inside the pipeline's finally block, BEFORE file cleanup.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Awaitable, Callable, Optional

from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)

# ── Shared FloodWait cooldown ─────────────────────────────────────────────────
_flood_cooldown_until: float = 0.0
_flood_lock = asyncio.Lock()


async def _set_flood_cooldown(seconds: float) -> None:
    async with _flood_lock:
        global _flood_cooldown_until
        candidate = time.time() + seconds
        if candidate > _flood_cooldown_until:
            _flood_cooldown_until = candidate
            logger.info("[upload] Shared FloodWait cooldown set: %.0f s", seconds)


async def _wait_flood_cooldown() -> None:
    async with _flood_lock:
        remaining = _flood_cooldown_until - time.time()
    if remaining > 0:
        logger.info("[upload] Shared FloodWait cooldown: waiting %.1f s", remaining)
        await asyncio.sleep(remaining)


# ── Core: FloodWait-safe upload ───────────────────────────────────────────────

async def upload_with_floodwait(
    send_coro: Callable[[], Awaitable],
    job_id: str,
    max_attempts: int = 4,
    status_msg=None,
    metrics=None,
) -> Optional[object]:
    """
    Execute send_coro() with FloodWait handling and retry.

    send_coro  : Zero-arg async callable — called fresh on each attempt.
    metrics    : Optional JobMetrics instance to record floodwait seconds.

    Returns the Telegram Message on success, or None if all attempts fail.
    The job MUST check for None and return early — never mark done otherwise.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        await _wait_flood_cooldown()

        try:
            result = await send_coro()
            if attempt > 1:
                logger.info("[upload] job=%s Upload successful after retry (attempt %d)",
                            job_id, attempt)
            return result

        except FloodWait as fw:
            wait_sec = int(getattr(fw, "value", None) or getattr(fw, "x", 60) or 60)
            wait_sec = max(wait_sec, 1)
            last_exc = fw
            logger.warning("[upload] job=%s FloodWait=%ds (attempt %d/%d)",
                           job_id, wait_sec, attempt, max_attempts)
            if metrics:
                metrics.record_floodwait(wait_sec)
            if attempt >= max_attempts:
                break
            await _set_flood_cooldown(wait_sec)
            await _notify_upload_retry(status_msg, job_id, attempt, max_attempts, wait_sec)
            logger.info("[upload] job=%s requeued for retry after cooldown", job_id)
            await asyncio.sleep(wait_sec)
            logger.info("[upload] job=%s retry scheduled after cooldown", job_id)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            # Non-FloodWait: fail fast — blind retry risks duplicate uploads
            last_exc = exc
            logger.error("[upload] job=%s Upload error (type=%s): %s",
                         job_id, type(exc).__name__, exc)
            break

    logger.error("[upload] job=%s All %d attempt(s) failed. last=%s",
                 job_id, max_attempts, last_exc)
    return None


async def _notify_upload_retry(status_msg, job_id, attempt, max_attempts, wait_sec):
    if not status_msg:
        return
    try:
        await status_msg.edit(
            f"╭━━━〔 ⏳ UPLOAD FLOOD WAIT 〕━━━╮\n"
            f"┃  🆔  <code>{job_id}</code>\n"
            f"┃  ⚠️  Attempt {attempt}/{max_attempts}\n"
            f"┃  📡  Telegram rate limit\n"
            f"┃  ⏳  Waiting {wait_sec}s before retry…\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )
    except Exception:
        pass


# ── MediaInfo helper (shared, runs once per job) ──────────────────────────────

async def generate_mediainfo_telegraph(
    final_file_path: str,
    display_name: str,
    file_size: int,
    job_id: str,
    metrics=None,
) -> Optional[str]:
    """
    Run ffprobe + Telegraph upload for the given local file.
    Returns Telegraph URL on success, None on any failure.
    Non-fatal: caller should never fail the job because of this.
    """
    if not final_file_path or not os.path.exists(final_file_path):
        logger.info("[mediainfo] job=%s skipped — file not available", job_id)
        return None

    if metrics:
        metrics.start_mediainfo()

    try:
        from plugins.mediainfo import (
            _ffprobe_sync,
            _build_telegraph_nodes,
            _upload_to_telegraph,
        )
        from helper.ffmpeg import run_blocking
        from config import Config
        bot_username = getattr(Config, "BOT_USERNAME", None) or "RimuruBot"

        logger.info("[mediainfo] job=%s ffprobe on %s", job_id, final_file_path)
        data  = await run_blocking(_ffprobe_sync, final_file_path)
        nodes = _build_telegraph_nodes(data, display_name, file_size, bot_username)
        url   = await _upload_to_telegraph(f"MediaInfo of {display_name}", nodes, bot_username)
        if url:
            logger.info("[mediainfo] job=%s Telegraph: %s", job_id, url)
        return url
    except Exception as e:
        logger.warning("[mediainfo] job=%s failed (non-fatal): %s", job_id, e)
        return None
    finally:
        if metrics:
            metrics.end_mediainfo()


# ── USER DUMP ─────────────────────────────────────────────────────────────────

async def dump_to_user_channel(
    client,
    user_id: int,
    channel_id: int,
    sent_msg,
    new_name: str,
    job_id: str,
    final_file_path: str = "",
    metrics=None,
) -> None:
    """
    Copy renamed file to the user's /dump channel.
    Caption = final filename ONLY (per spec).

    Uses Telegram-side copy_message — no re-upload of local file.
    """
    if metrics:
        metrics.start_dump()

    caption = new_name  # filename only

    async def _send():
        return await client.copy_message(
            chat_id=channel_id,
            from_chat_id=sent_msg.chat.id,
            message_id=sent_msg.id,
            caption=caption,
        )

    result = await upload_with_floodwait(_send, job_id=f"{job_id}_userdump")

    if metrics:
        metrics.end_dump()

    if result:
        logger.info("[dump:user] job=%s → channel=%s msg=%s", job_id, channel_id, result.id)
    else:
        logger.error("[dump:user] job=%s Failed to copy to channel=%s", job_id, channel_id)
        try:
            await client.send_message(
                user_id,
                f"⚠️ Could not dump to channel <code>{channel_id}</code>. "
                "Check bot is admin there.",
                disable_notification=True,
            )
        except Exception:
            pass


# ── BOT DUMP ──────────────────────────────────────────────────────────────────

async def dump_to_bot_channel(
    client,
    user_id: int,
    channel_id: int,
    sent_msg,
    original_name: str,
    new_name: str,
    username_str: str,
    job_id: str,
    file_size: int,
    final_file_path: str = "",
    ul_client=None,
    bin_sent=None,
    metrics=None,
) -> None:
    """
    Copy renamed file to the bot's BIN/LOG channel with rich FILE INFO caption,
    then add a 📊 MediaInfo inline button.

    For large files where bin_sent is already in BIN_CHANNEL:
      → edits caption only (no second upload).
    For normal files:
      → copy_message (Telegram-side — no re-upload of local file).

    MediaInfo is generated ONCE here and awaited before returning, so the
    caller can safely delete the local file after this coroutine returns.
    """
    from helper.utils import humanbytes as _hb
    size_str = _hb(file_size) if file_size else "N/A"
    job_str  = f"<code>{job_id}</code>" if job_id else "N/A"

    rich_caption = (
        "╭━━━〔 📂 FILE INFO 〕━━━╮\n\n"
        f"📂 Original:\n<code>{original_name or 'N/A'}</code>\n\n"
        f"➜ ✏️ Renamed:\n<code>{new_name or 'N/A'}</code>\n\n"
        f"👤 User: {username_str}\n"
        f"🆔 ID: <code>{user_id}</code>\n\n"
        f"🆔 Job: {job_str}\n"
        f"📦 Size: {size_str}\n"
        f"📊 Status: Completed\n\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━╯"
    )

    dump_msg = None

    if bin_sent and ul_client:
        # Large-file: already in BIN_CHANNEL via userbot — edit caption only.
        # One Telegram API call instead of a full copy.
        try:
            await ul_client.edit_message_caption(
                bin_sent.chat.id, bin_sent.id, caption=rich_caption,
            )
            dump_msg = bin_sent
            logger.info("[dump:bot] job=%s edited BIN_CHANNEL caption msg=%s", job_id, bin_sent.id)
        except Exception as e:
            logger.warning("[dump:bot] job=%s caption edit failed: %s", job_id, e)
    else:
        # Normal file: Telegram-side copy (no local file re-read).
        async def _send():
            return await client.copy_message(
                chat_id=channel_id,
                from_chat_id=sent_msg.chat.id,
                message_id=sent_msg.id,
                caption=rich_caption,
            )
        dump_msg = await upload_with_floodwait(_send, job_id=f"{job_id}_botdump")
        if dump_msg:
            logger.info("[dump:bot] job=%s → channel=%s msg=%s", job_id, channel_id, dump_msg.id)
        else:
            logger.error("[dump:bot] job=%s Failed to copy to channel=%s", job_id, channel_id)

    if not dump_msg:
        return

    # ── MediaInfo: generate ONCE and await (no fire-and-forget) ──────────────
    # This is awaited here so the pipeline knows when it's safe to delete files.
    display_name  = new_name or (os.path.basename(final_file_path) if final_file_path else "file")
    telegraph_url = await generate_mediainfo_telegraph(
        final_file_path, display_name, file_size, job_id, metrics=metrics
    )

    if not telegraph_url:
        return

    try:
        _chat_id = dump_msg.chat.id if hasattr(dump_msg, "chat") else channel_id
        await client.edit_message_reply_markup(
            chat_id=_chat_id,
            message_id=dump_msg.id,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📊 MediaInfo", url=telegraph_url),
            ]]),
        )
        logger.info("[dump:bot] job=%s 📊 MediaInfo button added", job_id)
    except Exception as btn_err:
        logger.warning("[dump:bot] job=%s MediaInfo button failed (non-fatal): %s",
                       job_id, btn_err)
