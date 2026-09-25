"""
worker/helper/pipeline.py
══════════════════════════════════════════════════════════════════════════════
Worker job pipeline — the file-processing heart of the distributed arch.

Adapts _run_pipeline() from the original auto_rename.py to work without a
live Telegram Message object.  Instead it receives a task dict from the
protocol layer (TASK message) which contains all the info needed:

  task = {
      "job_id":             str,
      "batch_id":           str,
      "user_id":            int,
      "source_chat_id":     int,
      "source_message_id":  int,
      "rename_pattern":     str,   # the computed final filename (Manager already resolved template)
      "prefix":             str,
      "suffix":             str,
      "metadata":           dict,  # {title, author, artist, audio, video, subtitle, comment}
                                   # already merged (global override applied by Manager)
      "metadata_version":   int,
      "thumbnail_url":      str | None,   # ImgBB HTTPS URL or None
      "dump_enabled":       bool,
  }

Pipeline steps
──────────────
  1. Fetch source message from Telegram (bot client; source chat)
  2. Send STATE=DOWNLOADING
  3. Download file via download_with_retry (bot client; 4 attempts)
  4. Send STATE=PROCESSING
  5. Embed metadata via FFmpeg if metadata dict is non-empty
  6. Download / resolve thumbnail from ImgBB URL (if any)
  7. Compute final caption and apply prefix/suffix
  8. Send STATE=UPLOADING
  9. Upload to Worker Output Channel (bot client for ≤2 GB;
     NOT userbot — the String Session is protocol-only in distributed arch)
 10. Send RESULT with (output_chat_id, output_message_id, filename, file_size)
 11. Clean up local temp files

Error handling
──────────────
  Any unrecoverable error at any step calls proto.send_failed() and cleans up.
  CancelledError is re-raised after cleanup so the task terminates cleanly.

Boundary rule (CRITICAL)
─────────────────────────
  • The String Session (proto_handler._session) is NEVER used here.
  • All download_media / send_document / send_video / send_audio calls use
    self._bot (the Worker Bot client).
  • The pipeline imports nothing from helper.protocol_handler — it only
    accepts send_state / send_result / send_failed as injected callables.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Callable, Awaitable, Optional

from config import Config
from helper.reliable_download import download_with_retry, DownloadFailedError
from helper.ffmpeg import add_metadata, get_duration_hachoir
from helper.utils import add_prefix_suffix, humanbytes, convert
from helper.upload_manager import upload_with_floodwait
from shared.protocol import (
    STATE_DOWNLOADING, STATE_PROCESSING, STATE_UPLOADING, STATE_UPLOADED,
)

logger = logging.getLogger(__name__)

_PROGRESS_THROTTLE = 3   # seconds between progress edits


class JobPipeline:
    """
    Stateless job executor.  One instance is typically shared; each call to
    run() is fully independent.

    Parameters
    ──────────
    bot_client    : Pyrogram Client (BOT_TOKEN) — all file I/O goes through this
    send_state    : Callable(job_id, state) — sends a STATE protocol message
    send_result   : Callable(**kwargs) — sends a RESULT protocol message
    send_failed   : Callable(job_id, reason) — sends a FAILED protocol message
    """

    def __init__(
        self,
        bot_client,
        send_state:  Callable[[str, str], Awaitable[None]],
        send_result: Callable[..., Awaitable[None]],
        send_failed: Callable[[str, str], Awaitable[None]],
    ):
        self._bot         = bot_client
        self._send_state  = send_state
        self._send_result = send_result
        self._send_failed = send_failed

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self, task: dict) -> None:
        job_id = task.get("job_id", "?")
        try:
            await self._run(task)
        except asyncio.CancelledError:
            logger.info("[pipeline] job=%s cancelled", job_id)
            await self._send_failed(job_id, "Job cancelled")
            raise
        except Exception as exc:
            logger.exception("[pipeline] job=%s unhandled exception: %s", job_id, exc)
            await self._send_failed(job_id, f"{type(exc).__name__}: {exc}")

    # ──────────────────────────────────────────────────────────────────────────
    # Internal pipeline
    # ──────────────────────────────────────────────────────────────────────────

    async def _run(self, task: dict) -> None:
        job_id            = task["job_id"]
        user_id           = int(task["user_id"])
        source_chat_id    = int(task.get("source_chat_id", 0))
        source_message_id = int(task.get("source_message_id", 0))
        rename_pattern    = task.get("rename_pattern", "")
        prefix            = task.get("prefix", "")
        suffix            = task.get("suffix", "")
        metadata          = task.get("metadata") or {}
        thumbnail_url     = task.get("thumbnail_url")

        download_path: Optional[str] = None
        metadata_path: Optional[str] = None
        thumb_path:    Optional[str] = None

        try:
            # ── 1. Fetch source message via bot client ────────────────────────
            if not source_chat_id or not source_message_id:
                await self._send_failed(job_id, "Missing source_chat_id or source_message_id")
                return
            try:
                message = await self._bot.get_messages(source_chat_id, source_message_id)
            except Exception as exc:
                await self._send_failed(job_id, f"Cannot fetch source message: {exc}")
                return
            if not message or not message.media:
                await self._send_failed(job_id, "Source message has no media")
                return

            # ── Identify file object ──────────────────────────────────────────
            if message.document:
                file_obj  = message.document
                base_name = file_obj.file_name or "file"
                base_type = "document"
            elif message.video:
                file_obj  = message.video
                base_name = file_obj.file_name or "video.mp4"
                base_type = "video"
            elif message.audio:
                file_obj  = message.audio
                base_name = file_obj.file_name or "audio.mp3"
                base_type = "audio"
            else:
                await self._send_failed(job_id, "Unsupported media type")
                return

            file_caption = (message.caption or "").strip()
            file_size    = getattr(file_obj, "file_size", 0) or 0

            # Size gate — Worker Bot is always a normal bot (2 GB limit)
            # Files > 2 GB are not supported in distributed mode without
            # a premium userbot; Manager should not assign them to a worker.
            # Guard here as a safety net.
            if file_size > Config.BOT_MAX_SIZE:
                await self._send_failed(
                    job_id,
                    f"File too large for worker bot ({humanbytes(file_size)} > 2 GB)"
                )
                return

            # ── 2. Resolve final filename ─────────────────────────────────────
            # Manager sends rename_pattern which is either the user's manual
            # filename or the fully-rendered template result (Manager applied
            # the episode/season/quality parser against the source filename).
            # Worker just applies prefix/suffix to the pattern.
            if not rename_pattern:
                rename_pattern = base_name

            final_name = add_prefix_suffix(rename_pattern, prefix, suffix)

            # ── Paths (isolated per job) ──────────────────────────────────────
            folder        = os.path.join("downloads", str(user_id), job_id)
            metadata_dir  = os.path.join("Metadata",  str(user_id), job_id)
            os.makedirs(folder,       exist_ok=True)
            os.makedirs(metadata_dir, exist_ok=True)

            download_path = os.path.join(folder,       final_name)
            metadata_path = os.path.join(metadata_dir, final_name)

            # ── 3. Check metadata need — decide fast-path vs full-path ──────
            has_meta = bool(metadata) and any(
                (v or "").strip() for v in metadata.values()
            )
            logger.info(
                "[pipeline] job=%s has_meta=%s  metadata_keys=%s  metadata=%s",
                job_id, has_meta, list(metadata.keys()), metadata,
            )

            if not has_meta:
                # ═══════════════════════════════════════════════════════════
                # NO-METADATA PATH: download → rename locally → re-upload.
                #
                # Telegram's file_id re-send (copy_message / send_document
                # with a file_id) IGNORES the file_name parameter — the name
                # is baked into the file_id on Telegram's servers and cannot
                # be changed without re-uploading the raw bytes.  The only
                # reliable way to rename is:
                #   1. Download to a local path named final_name
                #   2. Upload that local file (Telegram reads the filename
                #      from the path / file_name arg of the multipart upload)
                # ═══════════════════════════════════════════════════════════
                await self._send_state(job_id, STATE_DOWNLOADING)
                logger.info(
                    "[pipeline] job=%s NO-META PATH — download+rename+upload  size=%s",
                    job_id, humanbytes(file_size),
                )

                out_channel = Config.WORKER_OUTPUT_CHANNEL_ID
                last_prog_edit = [0.0]

                async def _progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    logger.debug("[pipeline] job=%s DL %d%%", job_id, pct)

                try:
                    file_path = await download_with_retry(
                        client        = self._bot,
                        message       = file_obj,
                        file_name     = download_path,
                        expected_size = file_size,
                        progress      = _progress,
                        max_attempts  = 4,
                        job_id        = job_id,
                    )
                except DownloadFailedError as exc:
                    await self._send_failed(
                        job_id,
                        f"Download failed after {exc.attempts} attempt(s): {exc.last_exc}",
                    )
                    return

                if not file_path or not os.path.exists(file_path):
                    await self._send_failed(job_id, "Download produced no file")
                    return

                actual_size = os.path.getsize(file_path)
                if actual_size == 0:
                    await self._send_failed(job_id, "Downloaded file is 0 bytes")
                    return

                if thumbnail_url:
                    try:
                        thumb_path = await self._download_thumbnail(
                            thumbnail_url, folder, job_id
                        )
                    except Exception as exc:
                        logger.warning(
                            "[pipeline] job=%s Thumb download failed: %s", job_id, exc
                        )
                        thumb_path = None

                if not thumb_path and base_type == "video":
                    if message.video and message.video.thumbs:
                        try:
                            thumb_path = await self._bot.download_media(
                                message.video.thumbs[0].file_id,
                                file_name=os.path.join(folder, f"thumb_{job_id}.jpg"),
                            )
                        except Exception:
                            pass

                duration = 0
                if base_type in ("video", "audio"):
                    try:
                        duration = await get_duration_hachoir(file_path)
                    except Exception:
                        pass

                _cap = f"<b>{final_name}</b>"
                await self._send_state(job_id, STATE_UPLOADING)
                logger.info(
                    "[pipeline] job=%s UPLOADING  type=%s  size=%s",
                    job_id, base_type, humanbytes(actual_size),
                )

                last_prog_edit[0] = 0.0
                c_time = time.time()

                async def _ul_progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    spd = cur / max(time.time() - c_time, 1)
                    logger.debug(
                        "[pipeline] job=%s UL %d%%  %s/s",
                        job_id, pct, humanbytes(int(spd)),
                    )

                if base_type == "document":
                    async def _ul_coro():
                        return await self._bot.send_document(
                            out_channel,
                            document=file_path,
                            file_name=final_name,
                            thumb=thumb_path,
                            caption=_cap,
                            progress=_ul_progress,
                        )
                elif base_type == "video":
                    async def _ul_coro():
                        return await self._bot.send_video(
                            out_channel,
                            video=file_path,
                            thumb=thumb_path,
                            caption=_cap,
                            duration=int(duration) if duration else None,
                            progress=_ul_progress,
                        )
                else:
                    async def _ul_coro():
                        return await self._bot.send_audio(
                            out_channel,
                            audio=file_path,
                            thumb=thumb_path,
                            caption=_cap,
                            duration=int(duration) if duration else None,
                            progress=_ul_progress,
                        )

                sent = await upload_with_floodwait(_ul_coro, job_id=job_id, status_msg=None)

                if not sent:
                    await self._send_failed(
                        job_id,
                        "Upload to output channel failed (FloodWait retries exhausted)",
                    )
                    return

                await self._send_state(job_id, STATE_UPLOADED)
                logger.info(
                    "[pipeline] job=%s Uploaded to output channel  msg_id=%s",
                    job_id, sent.id,
                )
                meta_applied = False

            else:
                # ═══════════════════════════════════════════════════════════
                # FULL PATH: metadata injection needed — must download,
                # embed with FFmpeg, then re-upload.
                # ═══════════════════════════════════════════════════════════
                await self._send_state(job_id, STATE_DOWNLOADING)
                logger.info(
                    "[pipeline] job=%s FULL PATH — download+embed+upload  size=%s",
                    job_id, humanbytes(file_size),
                )

                last_prog_edit = [0.0]

                async def _progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    logger.debug("[pipeline] job=%s DL %d%%", job_id, pct)

                try:
                    file_path = await download_with_retry(
                        client        = self._bot,
                        message       = file_obj,
                        file_name     = download_path,
                        expected_size = file_size,
                        progress      = _progress,
                        max_attempts  = 4,
                        job_id        = job_id,
                    )
                except DownloadFailedError as exc:
                    await self._send_failed(
                        job_id,
                        f"Download failed after {exc.attempts} attempt(s): {exc.last_exc}",
                    )
                    return

                if not file_path or not os.path.exists(file_path):
                    await self._send_failed(job_id, "Download produced no file")
                    return

                await self._send_state(job_id, STATE_PROCESSING)

                meta_applied = False
                result = await add_metadata(file_path, metadata_path, metadata, None)
                if result and os.path.exists(metadata_path):
                    file_path    = metadata_path
                    meta_applied = True
                else:
                    logger.error(
                        "[pipeline] job=%s Metadata embed FAILED — ffmpeg returned no output", job_id
                    )
                    await self._send_failed(job_id, "Metadata injection failed — FFmpeg returned an error")
                    return

                duration = 0
                try:
                    duration = await get_duration_hachoir(file_path)
                except Exception:
                    pass

                if thumbnail_url:
                    try:
                        thumb_path = await self._download_thumbnail(
                            thumbnail_url, folder, job_id
                        )
                    except Exception as exc:
                        logger.warning(
                            "[pipeline] job=%s Thumb download failed: %s", job_id, exc
                        )
                        thumb_path = None

                if not thumb_path and base_type == "video":
                    if message.video and message.video.thumbs:
                        try:
                            thumb_path = await self._bot.download_media(
                                message.video.thumbs[0].file_id,
                                file_name=os.path.join(folder, f"thumb_{job_id}.jpg"),
                            )
                        except Exception:
                            pass

                actual_size = os.path.getsize(file_path) if os.path.exists(file_path) else file_size
                if actual_size == 0:
                    await self._send_failed(job_id, "Processed file is 0 bytes")
                    return

                caption     = f"<b>{final_name}</b>"
                out_channel = Config.WORKER_OUTPUT_CHANNEL_ID

                await self._send_state(job_id, STATE_UPLOADING)
                logger.info(
                    "[pipeline] job=%s UPLOADING  type=%s  size=%s",
                    job_id, base_type, humanbytes(actual_size),
                )

                last_prog_edit[0] = 0.0
                c_time = time.time()

                async def _ul_progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    spd = cur / max(time.time() - c_time, 1)
                    logger.debug(
                        "[pipeline] job=%s UL %d%%  %s/s",
                        job_id, pct, humanbytes(int(spd)),
                    )

                if base_type == "document":
                    async def _ul_coro():
                        return await self._bot.send_document(
                            out_channel,
                            document=file_path,
                            file_name=final_name,
                            thumb=thumb_path,
                            caption=caption,
                            progress=_ul_progress,
                        )
                elif base_type == "video":
                    async def _ul_coro():
                        return await self._bot.send_video(
                            out_channel,
                            video=file_path,
                            thumb=thumb_path,
                            caption=caption,
                            duration=int(duration) if duration else None,
                            progress=_ul_progress,
                        )
                else:
                    async def _ul_coro():
                        return await self._bot.send_audio(
                            out_channel,
                            audio=file_path,
                            thumb=thumb_path,
                            caption=caption,
                            duration=int(duration) if duration else None,
                            progress=_ul_progress,
                        )

                sent = await upload_with_floodwait(_ul_coro, job_id=job_id, status_msg=None)

                if not sent:
                    await self._send_failed(
                        job_id,
                        "Upload to output channel failed (FloodWait retries exhausted)",
                    )
                    return

                await self._send_state(job_id, STATE_UPLOADED)
                logger.info(
                    "[pipeline] job=%s Uploaded to output channel  msg_id=%s",
                    job_id, sent.id,
                )

            # ── 6. Get MediaInfo URL (non-fatal) ──────────────────────────────
            mediainfo_url: Optional[str] = None
            try:
                from plugins.mediainfo import run_mediainfo_and_telegraph
                bot_me    = await self._bot.get_me()
                bot_uname = bot_me.username or "RenameWorkerBot"
                mi_path   = (
                    metadata_path
                    if meta_applied and os.path.exists(metadata_path)
                    else file_path
                )
                mediainfo_url = await run_mediainfo_and_telegraph(mi_path, final_name, bot_uname)
            except Exception as exc:
                logger.debug("[pipeline] job=%s MediaInfo failed (non-fatal): %s", job_id, exc)

            # ── 7. Send RESULT ────────────────────────────────────────────────
            await self._send_result(
                job_id            = job_id,
                output_chat_id    = sent.chat.id,
                output_message_id = sent.id,
                filename          = final_name,
                original_filename = base_name,
                file_size         = actual_size,
                mediainfo_url     = mediainfo_url,
            )

            logger.info("[pipeline] job=%s DONE  file=%s", job_id, final_name)

        finally:
            # ── 8. Cleanup temp files ─────────────────────────────────────────
            for _p in [download_path, thumb_path]:
                if _p and os.path.exists(_p):
                    try:
                        os.remove(_p)
                    except OSError:
                        pass
            if (metadata_path
                    and metadata_path != download_path
                    and os.path.exists(metadata_path)):
                try:
                    os.remove(metadata_path)
                except OSError:
                    pass
            for _d in [
                os.path.join("downloads", str(user_id), job_id),
                os.path.join("Metadata",  str(user_id), job_id),
            ]:
                try:
                    if os.path.isdir(_d) and not os.listdir(_d):
                        os.rmdir(_d)
                except OSError:
                    pass

    # ──────────────────────────────────────────────────────────────────────────
    # Thumbnail helper
    # ──────────────────────────────────────────────────────────────────────────

    async def _download_thumbnail(
        self, url: str, folder: str, job_id: str
    ) -> Optional[str]:
        """Download a thumbnail from an HTTPS URL (ImgBB) to a local file."""
        import aiohttp
        dest = os.path.join(folder, f"thumb_{job_id}.jpg")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    raise ValueError(f"HTTP {resp.status} fetching thumbnail")
                data = await resp.read()
        with open(dest, "wb") as f:
            f.write(data)
        return dest if os.path.getsize(dest) > 0 else None
