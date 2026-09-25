"""
helper/ffmpeg.py
─────────────────
FFmpeg/FFprobe helpers — every CPU-bound / subprocess call is async-safe.

run_blocking(func, *args)
  Routes sync functions through loop.run_in_executor so they NEVER block
  the event loop.  asyncio.create_subprocess_exec is used for all
  subprocess calls so FFmpeg never holds the event loop either.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import random
import time
from typing import Any

from messages import log, Msg


# ══════════════════════════════════════════════════════════════════════════════
# run_blocking — universal bridge from sync → async
# ══════════════════════════════════════════════════════════════════════════════

async def run_blocking(func, *args, **kwargs) -> Any:
    """
    Run a synchronous (blocking) callable in the default thread-pool executor
    so it NEVER stalls the event loop.

    Usage:
        result = await run_blocking(my_sync_func, arg1, arg2)

    For keyword-argument functions wrap with functools.partial first:
        result = await run_blocking(functools.partial(my_func, key=val), arg1)
    """
    loop = asyncio.get_running_loop()
    if kwargs:
        func = functools.partial(func, **kwargs)
    return await loop.run_in_executor(None, func, *args)


# ══════════════════════════════════════════════════════════════════════════════
# fix_thumb — PIL in thread pool via run_blocking
# ══════════════════════════════════════════════════════════════════════════════

def _fix_thumb_sync(thumb: str):
    """Sync PIL worker — called via run_blocking, never touches event loop."""
    from hachoir.metadata import extractMetadata
    from hachoir.parser import createParser
    from PIL import Image

    width = height = 0
    parser = createParser(thumb)
    if parser:
        meta = extractMetadata(parser)
        if meta:
            if meta.has("width"):
                width = meta.get("width")
            if meta.has("height"):
                height = meta.get("height")
        parser.stream._input.close()

    img = Image.open(thumb).convert("RGB")
    if width and height:
        img = img.resize((width, height), Image.LANCZOS)
    else:
        width, height = img.size
    img.save(thumb, "JPEG", subsampling=0, quality=95)
    return width, height, thumb


async def fix_thumb(thumb: str):
    """
    Ensure *thumb* is a valid Baseline JPEG.
    Returns (width, height, path). path is None on failure.
    Non-blocking: PIL/hachoir run in thread pool via run_blocking.

    Guards:
      • Empty path           → (0, 0, None)
      • File does not exist  → (0, 0, None)
      • File is 0 bytes      → (0, 0, None)  ← fixes hachoir "Input size is nul"
    """
    if not thumb:
        return 0, 0, None
    if not os.path.exists(thumb):
        log.warning(Msg.THUMB_FIX_ERR, error=f"thumb file not found: {thumb}")
        return 0, 0, None
    if os.path.getsize(thumb) == 0:
        log.warning(Msg.THUMB_FIX_ERR, error=f"thumb is 0 bytes: {thumb}")
        try:
            os.remove(thumb)
        except Exception:
            pass
        return 0, 0, None
    try:
        return await run_blocking(_fix_thumb_sync, thumb)
    except Exception as e:
        log.error(Msg.THUMB_FIX_ERR, error=e)
        return 0, 0, None


# ══════════════════════════════════════════════════════════════════════════════
# take_screen_shot — async subprocess (never blocks event loop)
# ══════════════════════════════════════════════════════════════════════════════

async def take_screen_shot(video_file: str, output_directory: str, ttl: int):
    """
    Extract a single frame at *ttl* seconds.
    -y      : overwrite output without prompting (prevents hang)
    -i first: accurate seeking (avoids missing frames near start of file)
    -ss after -i: frame-accurate seek (slower but correct for short seeks)
    """
    out_file = os.path.join(output_directory, f"{time.time()}.jpg")
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-i", video_file,
        "-ss", str(ttl),
        "-vframes", "1",
        "-q:v", "2",
        out_file,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.communicate()
    return out_file if os.path.exists(out_file) and os.path.getsize(out_file) > 0 else None


# ══════════════════════════════════════════════════════════════════════════════
# get_video_duration — async ffprobe subprocess
# ══════════════════════════════════════════════════════════════════════════════

async def get_video_duration(file_path: str) -> float:
    """Return duration in seconds. Returns 0.0 on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "json", file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode(errors="replace"))
        return float(data["format"]["duration"])
    except Exception as e:
        log.warning(Msg.FFPROBE_DUR_ERR, error=e)
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# get_duration_hachoir — hachoir in thread pool via run_blocking
# ══════════════════════════════════════════════════════════════════════════════

def _hachoir_duration_sync(file_path: str) -> int:
    from hachoir.metadata import extractMetadata
    from hachoir.parser import createParser
    try:
        parser = createParser(file_path)
        if parser:
            meta = extractMetadata(parser)
            secs = meta.get("duration").seconds if (meta and meta.has("duration")) else 0
            parser.stream._input.close()
            return secs
    except Exception:
        pass
    return 0


async def get_duration_hachoir(file_path: str) -> int:
    """Non-blocking hachoir duration extraction via run_blocking."""
    return await run_blocking(_hachoir_duration_sync, file_path)


# ══════════════════════════════════════════════════════════════════════════════
# add_metadata — async ffmpeg subprocess
# ✔ asyncio.create_subprocess_exec → never blocks event loop
# ══════════════════════════════════════════════════════════════════════════════

async def add_metadata(
    input_path: str,
    output_path: str,
    metadata_fields: dict,
    ms,
) -> str | None:
    """
    Embed metadata tags into *input_path* → *output_path*.
    Stream copy — no re-encode. Returns output_path on success, None on failure.

    Worker-safe: pass ms=None when there is no status message to edit.
    When ms is None, progress edits are skipped entirely (no AttributeError).

    Deadlock fix: stdout (-progress pipe:1) and stderr are both PIPEs and are
    drained concurrently via asyncio.gather so neither buffer can fill up and
    block ffmpeg while the other side waits.
    """
    import math
    from helper.utils import humanbytes, TimeFormatter
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    try:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        # ── Get total duration via ffprobe ─────────────────────────────────────
        total_us: int = 0
        try:
            probe = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                input_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            probe_out, _ = await probe.communicate()
            probe_data   = json.loads(probe_out.decode(errors="replace"))
            dur_str      = probe_data.get("format", {}).get("duration", "0")
            total_us     = int(float(dur_str) * 1_000_000)
        except Exception:
            pass  # no duration → progress bar shows time elapsed only

        # ── Count streams via ffprobe to detect attachment-heavy files ──────────
        # Files with 100+ font/attachment streams (e.g. subbed anime MKVs) cause
        # ffmpeg to load all attachments into RAM simultaneously → OOM/exit -9
        # on memory-constrained hosts (Koyeb free = 512 MB).
        # Strategy: if attachment count > threshold, map only essential streams
        # (video, audio, subtitles) and drop attachment streams entirely.
        _MAX_SAFE_STREAMS = 10  # above this, selective mapping is used
        _stream_count = 0
        _has_heavy_attachments = False
        try:
            _sp = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                "-select_streams", "t",  # attachment streams only
                input_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            _sp_out, _ = await _sp.communicate()
            _attach_data = json.loads(_sp_out.decode(errors="replace"))
            _stream_count = len(_attach_data.get("streams", []))
            _has_heavy_attachments = _stream_count > _MAX_SAFE_STREAMS
        except Exception:
            pass

        # ── Build ffmpeg command with -progress pipe:1 ─────────────────────────
        cmd = [
            "ffmpeg", "-y",
            "-threads", "1",   # cap CPU threads → lower peak RSS on free tier
            "-i", input_path,
        ]

        if _has_heavy_attachments:
            # Selective mapping: video + audio + subtitles only; skip attachments.
            # -map 0:V? (capital V) maps real video streams but EXCLUDES streams
            # with the attached_pic disposition (e.g. cover.jpg embedded as mjpeg).
            # Using lowercase -map 0:v? would include cover art, which cannot be
            # copied into MKV as a regular video stream → "Could not write header".
            log.warning(
                "[add_metadata] {count} attachment streams detected — using selective map "
                "to avoid OOM (file: {filename})",
                count=_stream_count,
                filename=os.path.basename(input_path),
            )
            cmd += [
                "-map", "0:V?",   # real video only (excludes attached_pic cover art)
                "-map", "0:a?",   # all audio streams
                "-map", "0:s?",   # all subtitle streams
            ]
        else:
            cmd += ["-map", "0"]  # safe to copy everything

        cmd += ["-c", "copy"]

        for tag in ("title", "artist", "author", "comment"):
            val = (metadata_fields.get(tag) or "").strip()
            if val:
                cmd += ["-metadata", f"{tag}={val}"]

        audio_title = (metadata_fields.get("audio") or "").strip()
        video_title = (metadata_fields.get("video") or "").strip()
        sub_title   = (metadata_fields.get("subtitle") or "").strip()
        if audio_title:
            cmd += ["-metadata:s:a", f"title={audio_title}"]
        if video_title:
            cmd += ["-metadata:s:v", f"title={video_title}"]
        if sub_title:
            cmd += ["-metadata:s:s", f"title={sub_title}"]

        # -progress pipe:1 → key=value progress lines on stdout every ~0.5s
        cmd += ["-progress", "pipe:1", "-nostats", output_path]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # ── Drain stdout (progress) and stderr concurrently ──────────────────
        # CRITICAL: both stdout and stderr are PIPEs. Reading one to completion
        # while the other fills its OS buffer → ffmpeg blocks → deadlock.
        # Fix: drain both concurrently with asyncio.gather.
        start_time = time.time()
        last_edit  = 0.0
        file_size  = os.path.getsize(input_path)

        async def _drain_stdout():
            nonlocal last_edit
            out_time_us = 0
            async for raw_line in proc.stdout:
                line = raw_line.decode(errors="replace").strip()
                if line.startswith("out_time_us="):
                    try:
                        out_time_us = int(line.split("=", 1)[1])
                    except ValueError:
                        pass

                # Skip progress edits entirely when there is no status message
                if ms is None:
                    continue

                now = time.time()
                if (now - last_edit) < 5:
                    continue
                last_edit = now

                elapsed   = now - start_time
                pct       = (out_time_us / total_us * 100) if total_us > 0 else 0
                pct       = min(pct, 99.9)
                eta_str   = "..."

                if elapsed > 0 and pct > 0:
                    eta_s   = (elapsed / pct) * (100 - pct)
                    eta_str = TimeFormatter(milliseconds=int(eta_s * 1000))

                filled       = math.floor(pct / 5)
                progress_bar = "▣" * filled + "▢" * (20 - filled)

                bar_text = (
                    f"{progress_bar}\n\n"
                    f"<b>📦 Size :</b>  {humanbytes(file_size)}\n"
                    f"<b>✅ Done :</b>  {round(pct, 1)}%\n"
                    f"<b>⏱ ETA :</b>   {eta_str}\n"
                    f"<b>⏳ Time :</b>  {TimeFormatter(milliseconds=int(elapsed * 1000))}\n"
                )
                try:
                    await ms.edit(
                        text=f"🏷 Adding Metadata... ⚡\n\n{bar_text}",
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("✖️ Cancel", callback_data="close")]]
                        ),
                    )
                except Exception:
                    pass

        async def _drain_stderr() -> bytes:
            chunks = []
            async for line in proc.stderr:
                chunks.append(line)
            return b"".join(chunks)

        # Concurrent drain — neither pipe blocks the other
        _, stderr_bytes = await asyncio.gather(_drain_stdout(), _drain_stderr())
        await proc.wait()

        stderr_txt = stderr_bytes.decode(errors="replace").strip()
        if stderr_txt:
            log.debug(Msg.META_FFMPEG_STDERR, stderr=stderr_txt)

        if proc.returncode != 0:
            log.error(Msg.META_ERROR, error=f"ffmpeg exit {proc.returncode}: {stderr_txt[-300:]}")
            await _safe_edit(ms, "❌ Metadata injection failed.")
            return None

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            await _safe_edit(ms, "✅ Metadata added.")
            return output_path

        log.error(Msg.META_ERROR, error="output file missing or empty after ffmpeg")
        await _safe_edit(ms, "❌ Could not add metadata.")
        return None

    except Exception as e:
        log.error(Msg.META_ERROR, error=e)
        await _safe_edit(ms, f"❌ Metadata injection failed: `{e}`")
        return None


async def _safe_edit(ms, text: str) -> None:
    if ms is None:
        return
    try:
        await ms.edit(text)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# generate_sample_video — async subprocess, stream copy + re-encode fallback
# ══════════════════════════════════════════════════════════════════════════════

async def generate_sample_video(
    input_path: str,
    output_directory: str,
    duration: int = 30,
) -> str | None:
    """
    Cut a *duration*-second sample. Fully async subprocess.
    Never blocks the event loop.
    """
    total = await get_video_duration(input_path)

    if total <= 0:
        start = 0.0
    elif total <= duration:
        start = 0.0
    else:
        lo    = total * 0.10
        hi    = max(total * 0.70, lo + 1.0)
        start = random.uniform(lo, hi)
        start = min(start, total - duration - 0.5)

    out_file = os.path.join(output_directory, f"sample_{int(time.time())}.mp4")

    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        # -fflags +genpts: regenerate PTS for partial/non-seekable streams
        # -ignore_unknown: skip unrecognised streams (data/attachment tracks)
        "-fflags", "+genpts",
        "-ss", f"{start:.2f}", "-i", input_path,
        "-t", str(duration),
        "-map", "0",
        "-ignore_unknown",
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",
        out_file,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()

    if proc.returncode == 0 and os.path.exists(out_file) and os.path.getsize(out_file) > 0:
        return out_file

    log.warning(Msg.SAMPLE_COPY_FAIL, stderr=stderr.decode(errors="replace")[-200:])
    if os.path.exists(out_file):
        os.remove(out_file)

    # Re-encode fallback — works even on truncated partial streams
    proc2 = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-fflags", "+genpts+discardcorrupt",
        "-ss", f"{start:.2f}", "-i", input_path,
        "-t", str(duration),
        "-c:v", "libx264", "-c:a", "aac",
        "-preset", "ultrafast", "-crf", "28",
        "-movflags", "+faststart",
        out_file,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr2 = await proc2.communicate()

    if proc2.returncode != 0:
        log.error(Msg.SAMPLE_ENCODE_FAIL, stderr=stderr2.decode(errors="replace")[-200:])
        return None

    return out_file if os.path.exists(out_file) and os.path.getsize(out_file) > 0 else None


# ══════════════════════════════════════════════════════════════════════════════
# take_multi_screenshots — kept for internal use (raw frame list)
# ══════════════════════════════════════════════════════════════════════════════

async def take_multi_screenshots(
    video_file: str,
    output_directory: str,
    count: int = 6,
) -> list[tuple[str, float]]:
    """
    Capture *count* evenly-spaced frames concurrently via asyncio.gather.
    Returns list of (path, timestamp_seconds) tuples.
    All FFmpeg subprocesses run in parallel — never sequential.
    """
    duration = await get_video_duration(video_file)

    if duration <= 0:
        result = await take_screen_shot(video_file, output_directory, 0)
        return [(result, 0.0)] if result else []

    # Generate *count* evenly-spaced timestamps across the video.
    # Start at 2% (skip black intro) and end at 97% (skip credits).
    # Works for any count from 1 to 12 — no hardcoded list.
    if count <= 1:
        timestamps = [duration * 0.50]
    else:
        start_pct, end_pct = 0.02, 0.97
        step = (end_pct - start_pct) / (count - 1)
        timestamps = [duration * (start_pct + i * step) for i in range(count)]

    async def _one_shot(ts: float) -> tuple[str, float] | None:
        out = os.path.join(output_directory, f"ss_{int(time.time() * 1000)}_{ts:.0f}.jpg")
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-ss", f"{ts:.2f}", "-i", video_file,
            "-vframes", "1", "-q:v", "2", out,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        return (out, ts) if os.path.exists(out) and os.path.getsize(out) > 0 else None

    results = await asyncio.gather(*[_one_shot(ts) for ts in timestamps])
    return [r for r in results if r]


# ══════════════════════════════════════════════════════════════════════════════
# generate_screenshot_grid — combines 6 frames into one 3×2 grid image
#
# Layout (matches reference image):
#   [ frame1 | frame2 | frame3 ]
#   [ frame4 | frame5 | frame6 ]
#
# Each frame has its timestamp burned in the bottom-right corner:
#   HH:MM:SS  white text, black shadow for visibility on any background.
#
# PIL work runs in run_blocking() — never touches event loop.
# ══════════════════════════════════════════════════════════════════════════════

def _build_grid_sync(
    frames: list[tuple[str, float]],
    output_path: str,
    cols: int = 3,
    thumb_w: int = 426,
    thumb_h: int = 240,
    padding: int = 10,
    bg_color: tuple = (15, 15, 15),
) -> str:
    """
    Synchronous PIL worker — called via run_blocking().

    frames  : list of (image_path, seconds) — must have 1..6 entries
    output_path : where to save the final JPEG grid
    cols    : number of columns (3)
    thumb_w / thumb_h : size each frame is resized to
    padding : gap between frames and border
    bg_color: canvas background (near-black)
    """
    from PIL import Image, ImageDraw, ImageFont

    rows = (len(frames) + cols - 1) // cols

    canvas_w = cols * thumb_w + (cols + 1) * padding
    canvas_h = rows * thumb_h + (rows + 1) * padding
    canvas   = Image.new("RGB", (canvas_w, canvas_h), bg_color)

    # Try to load a monospace font; fall back to default
    font = None
    font_shadow = None
    for font_path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf",
        "/usr/share/fonts/TTF/DejaVuSansMono-Bold.ttf",
    ):
        if os.path.exists(font_path):
            try:
                font        = ImageFont.truetype(font_path, 18)
                font_shadow = font
                break
            except Exception:
                pass
    if font is None:
        font = ImageFont.load_default()

    for idx, (img_path, ts_sec) in enumerate(frames):
        row = idx // cols
        col = idx %  cols

        x = padding + col * (thumb_w + padding)
        y = padding + row * (thumb_h + padding)

        # Open + resize frame
        try:
            frame = Image.open(img_path).convert("RGB")
            frame = frame.resize((thumb_w, thumb_h), Image.LANCZOS)
        except Exception:
            # Blank frame on load error
            frame = Image.new("RGB", (thumb_w, thumb_h), (30, 30, 30))

        # ── Timestamp overlay ────────────────────────────────────────────────
        ts_str = _seconds_to_ts(ts_sec)
        draw   = ImageDraw.Draw(frame)

        # Measure text size
        try:
            bbox   = draw.textbbox((0, 0), ts_str, font=font)
            txt_w  = bbox[2] - bbox[0]
            txt_h  = bbox[3] - bbox[1]
        except AttributeError:
            # Older Pillow fallback
            txt_w, txt_h = draw.textsize(ts_str, font=font)

        tx = thumb_w - txt_w - 8
        ty = thumb_h - txt_h - 8

        # Shadow / outline (draw offset copies in black)
        for dx, dy in ((-1, -1), (1, -1), (-1, 1), (1, 1), (0, 1), (1, 0)):
            draw.text((tx + dx, ty + dy), ts_str, font=font, fill=(0, 0, 0))
        # White foreground
        draw.text((tx, ty), ts_str, font=font, fill=(255, 255, 255))

        canvas.paste(frame, (x, y))

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    canvas.save(output_path, "JPEG", quality=92, optimize=True)
    return output_path


def _seconds_to_ts(seconds: float) -> str:
    """Convert float seconds → HH:MM:SS string."""
    s   = int(seconds)
    h   = s // 3600
    m   = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


async def generate_screenshot_grid(
    video_file: str,
    output_directory: str,
    count: int = 6,
    cols: int = 3,
) -> str | None:
    """
    High-level async entry point:
      1. Capture *count* frames in parallel (async subprocesses).
      2. Build 3×2 grid with PIL in thread pool (run_blocking).
      3. Clean up raw frame files.
      4. Return path to the combined grid JPEG, or None on failure.

    This is the ONLY function callers should use — never send individual
    screenshots separately.
    """
    frames = await take_multi_screenshots(video_file, output_directory, count)
    if not frames:
        return None

    grid_path = os.path.join(output_directory, "screenshot_grid.jpg")
    try:
        result = await run_blocking(_build_grid_sync, frames, grid_path, cols)
    except Exception as e:
        log.error("Grid build failed: {error}", error=e)
        return None
    finally:
        # Clean up individual raw frames regardless of grid success/failure
        for img_path, _ in frames:
            try:
                if os.path.exists(img_path):
                    os.remove(img_path)
            except Exception:
                pass

    return result if os.path.exists(result) else None
