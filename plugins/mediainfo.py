"""
worker/plugins/mediainfo.py
══════════════════════════════════════════════════════════════════════════════
Standalone MediaInfo helper for the Worker — no Pyrogram command handler,
no DB dependency.  Called by pipeline.py step 6.

Public API
──────────
  run_mediainfo_and_telegraph(file_path, display_name) -> str | None
    Runs ffprobe, builds Telegraph nodes, uploads page, returns URL.
    Returns None on any failure — always non-fatal.

Ported verbatim from plugins/mediainfo.py — all the pure functions
(_ffprobe_sync, _build_telegraph_nodes, _upload_to_telegraph, helpers)
are identical.  Removed: @Client.on_message handler, jishubotz import,
_partial_download (worker already has the full file).
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess

import aiohttp
from helper.ffmpeg import run_blocking

logger = logging.getLogger(__name__)

# Module-level Telegraph token — created once, reused across calls
_telegraph_token: str | None = None


# ══════════════════════════════════════════════════════════════════════════════
# Public entry point called from pipeline.py
# ══════════════════════════════════════════════════════════════════════════════

async def run_mediainfo_and_telegraph(
    file_path:    str,
    display_name: str,
    bot_username: str = "RenameWorkerBot",
) -> str | None:
    """
    Run ffprobe on file_path, build a Telegraph page, return its URL.
    Returns None silently on any error — pipeline treats this as non-fatal.
    """
    if not file_path or not os.path.exists(file_path):
        return None
    try:
        file_size = os.path.getsize(file_path)
        data      = await run_blocking(_ffprobe_sync, file_path)
        if not data:
            return None
        nodes    = _build_telegraph_nodes(data, display_name, file_size, bot_username)
        page_url = await _upload_to_telegraph(
            f"MediaInfo of {display_name}", nodes, bot_username
        )
        return page_url
    except Exception as exc:
        logger.debug("[mediainfo] Non-fatal error for %s: %s", display_name, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# ffprobe (sync — called via run_blocking)
# ══════════════════════════════════════════════════════════════════════════════

def _ffprobe_sync(file_path: str) -> dict:
    cmd = [
        "ffprobe", "-v", "quiet",
        "-probesize", "50000000",
        "-analyzeduration", "10000000",
        "-print_format", "json",
        "-show_format", "-show_streams", "-show_chapters",
        file_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.stdout:
            return json.loads(result.stdout)
    except Exception as exc:
        logger.debug("[mediainfo] ffprobe error: %s", exc)
    return {}


# ══════════════════════════════════════════════════════════════════════════════
# Telegraph node builder — verbatim from original mediainfo.py
# ══════════════════════════════════════════════════════════════════════════════

def _build_telegraph_nodes(data, display_name, file_size, bot_username) -> list:
    fmt      = data.get("format", {})
    streams  = data.get("streams", [])
    chapters = data.get("chapters", [])
    tags_f   = fmt.get("tags", {})
    from datetime import datetime

    def h3(t):       return {"tag": "h3",   "children": [t]}
    def h4(t):       return {"tag": "h4",   "children": [t]}
    def p(*c):       return {"tag": "p",    "children": list(c)}
    def bold(t):     return {"tag": "b",    "children": [t]}
    def em(t):       return {"tag": "em",   "children": [t]}
    def code(t):     return {"tag": "code", "children": [t]}
    def br():        return {"tag": "br"}
    def hr():        return {"tag": "hr"}
    def link(u, t):  return {"tag": "a", "attrs": {"href": u}, "children": [t]}
    def li(*c):      return {"tag": "li",   "children": list(c)}
    def ol(items):   return {"tag": "ol",   "children": items}

    nodes = []
    date_str = datetime.utcnow().strftime("%B %d, %Y")
    nodes.append(p(link(f"https://t.me/{bot_username}", f"@{bot_username}"), f"  {date_str}"))
    nodes.append(h4(f"MediaInfo of {display_name}"))
    nodes.append(hr())

    # General
    nodes.append(h3("🎬 General Info"))
    fmt_name  = fmt.get("format_long_name") or fmt.get("format_name") or "N/A"
    dur       = float(fmt.get("duration") or 0)
    br_raw    = fmt.get("bit_rate", "")
    title_tag = tags_f.get("title") or tags_f.get("TITLE") or ""
    res_str   = ""
    for s in streams:
        if s.get("codec_type") == "video":
            w = s.get("width"); h_ = s.get("height")
            if w and h_:
                res_str = f"{w}x{h_}"
            break
    if title_tag: nodes.append(p(bold("Title: "), title_tag))
    nodes.append(p(bold("Format: "), fmt_name))
    if res_str:   nodes.append(p(bold("Resolution: "), res_str))
    if dur:       nodes.append(p(bold("Duration: "), _fmt_dur_long(int(dur))))
    nodes.append(p(bold("File Size: "), _humanbytes(file_size)))
    if br_raw:    nodes.append(p(bold("Bitrate: "), _fmt_br(br_raw)))
    nodes.append(hr())

    # Video
    video_streams = [s for s in streams if s.get("codec_type") == "video"]
    if video_streams:
        nodes.append(h3("🖼️ Video Stream"))
        s          = video_streams[0]
        stags      = s.get("tags", {})
        codec_long = s.get("codec_long_name", "")
        codec_name = s.get("codec_name", "?")
        profile    = s.get("profile", "")
        codec_str  = codec_long if codec_long else codec_name
        if codec_long: codec_str += f" ({codec_name})"
        if profile and profile not in ("unknown", ""): codec_str += f" - {profile}"
        pix_fmt   = s.get("pix_fmt", "")
        color_sp  = s.get("color_space", "")
        color_pr  = s.get("color_primaries", "")
        color_str = ", ".join(filter(None, [color_sp, color_pr]))
        dar       = s.get("display_aspect_ratio", "")
        fps_str   = _parse_fps(s.get("r_frame_rate", "")) or _parse_fps(s.get("avg_frame_rate", "")) or ""
        vtitle    = stags.get("title") or stags.get("TITLE") or ""
        if vtitle:    nodes.append(p(bold("Title: "), vtitle))
        nodes.append(p(bold("Codec: "), codec_str))
        if pix_fmt:   nodes.append(p(bold("Pixel Format: "), pix_fmt))
        if color_str: nodes.append(p(bold("Color: "), color_str))
        if dar and dar != "0:1": nodes.append(p(bold("Aspect Ratio: "), dar))
        if fps_str:   nodes.append(p(bold("Frame Rate: "), fps_str))
        nodes.append(hr())

    # Audio
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    if audio_streams:
        nodes.append(h3("🔊 Audio Tracks"))
        items = []
        for s in audio_streams:
            stags     = s.get("tags", {})
            lang      = _lang_display(stags.get("language") or stags.get("LANGUAGE") or "")
            disp      = s.get("disposition", {})
            flags     = (["Default"] if disp.get("default") else []) + (["Forced"] if disp.get("forced") else [])
            flag_str  = f" ({', '.join(flags)})" if flags else ""
            codec_long = s.get("codec_long_name", ""); codec_name = s.get("codec_name", "?")
            codec_str  = (codec_long + f" ({codec_name})") if codec_long else codec_name
            ch_layout  = s.get("channel_layout", ""); ch_count = s.get("channels", "")
            ch_str     = ch_layout if ch_layout else (f"{ch_count}ch" if ch_count else "")
            abr        = s.get("bit_rate", ""); sr = s.get("sample_rate", "")
            sr_str     = f"{int(float(sr)) // 1000}kHz" if sr else ""
            detail_parts = [codec_str]
            if ch_str: detail_parts.append(ch_str)
            if abr:    detail_parts.append(f"@ {_fmt_br(abr)}")
            if sr_str: detail_parts.append(sr_str)
            atitle = stags.get("title") or stags.get("TITLE") or ""
            item_ch = [bold(f"{lang}{flag_str} - "), " ".join(detail_parts)]
            if atitle: item_ch += [br(), em(f"  ‣ {atitle}")]
            items.append(li(*item_ch))
        nodes.append(ol(items)); nodes.append(hr())

    # Subtitles
    sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
    if sub_streams:
        nodes.append(h3("📝 Subtitle Tracks"))
        items = []
        for s in sub_streams:
            stags     = s.get("tags", {})
            lang      = _lang_display(stags.get("language") or stags.get("LANGUAGE") or "")
            disp      = s.get("disposition", {})
            flags     = (["Default"] if disp.get("default") else []) + (["Forced"] if disp.get("forced") else [])
            flag_str  = f" ({', '.join(flags)})" if flags else ""
            codec_long = s.get("codec_long_name", ""); codec_name = s.get("codec_name", "?")
            codec_str  = (codec_long + f" ({codec_name})") if codec_long else codec_name
            stitle    = stags.get("title") or stags.get("TITLE") or ""
            item_ch   = [bold(f"{lang}{flag_str} - "), em(codec_str)]
            if stitle: item_ch += [br(), f"  ‣ {stitle}"]
            items.append(li(*item_ch))
        nodes.append(ol(items)); nodes.append(hr())

    # Chapters
    if chapters:
        nodes.append(h3("🔖 Chapters"))
        ch_items = []
        for i, ch in enumerate(chapters, 1):
            ctags = ch.get("tags", {})
            title = ctags.get("title") or ctags.get("TITLE") or f"Chapter {i}"
            start = _fmt_timestamp(float(ch.get("start_time", 0)))
            end   = _fmt_timestamp(float(ch.get("end_time", 0)))
            ch_items.append(li(code(f"{title}:"), f" {start} - {end}"))
        nodes.append(ol(ch_items)); nodes.append(hr())

    # Technical
    writing_app = (tags_f.get("writing_application") or tags_f.get("WRITING_APPLICATION")
                   or tags_f.get("encoder") or tags_f.get("ENCODER") or "")
    encoded_by  = tags_f.get("encoded_by") or tags_f.get("ENCODED_BY") or ""
    nb_streams  = fmt.get("nb_streams", "")
    tech = []
    if writing_app: tech.append(p(bold("Muxed with: "), writing_app))
    if encoded_by:  tech.append(p(bold("Encoded By: "), encoded_by))
    if nb_streams:  tech.append(p(bold("Total Streams: "), str(nb_streams)))
    if tech:
        nodes.append(h3("🛠️ Technical"))
        nodes.extend(tech)

    return nodes


# ══════════════════════════════════════════════════════════════════════════════
# Telegraph uploader — verbatim from original
# ══════════════════════════════════════════════════════════════════════════════

async def _upload_to_telegraph(title: str, nodes: list, bot_username: str) -> str | None:
    global _telegraph_token
    _LIMIT = 60_000
    safe_nodes = list(nodes)
    while True:
        nodes_json = json.dumps(safe_nodes, ensure_ascii=False)
        if len(nodes_json.encode("utf-8")) <= _LIMIT:
            break
        if not safe_nodes:
            return None
        safe_nodes.pop()

    if not _telegraph_token:
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
                    async with s.post("https://api.telegra.ph/createAccount", data={
                        "short_name":  bot_username[:32],
                        "author_name": f"@{bot_username}",
                        "author_url":  f"https://t.me/{bot_username}",
                    }) as r:
                        d = json.loads(await r.text())
                        if d.get("ok"):
                            _telegraph_token = d["result"]["access_token"]
                            break
            except Exception:
                pass
            if attempt < 2:
                await asyncio.sleep(3)

    if not _telegraph_token:
        return None

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
                async with s.post("https://api.telegra.ph/createPage", data={
                    "access_token": _telegraph_token,
                    "title":        (title or "MediaInfo")[:256],
                    "author_name":  f"@{bot_username}",
                    "author_url":   f"https://t.me/{bot_username}",
                    "content":      nodes_json,
                }) as r:
                    result = json.loads(await r.text())
                    if result.get("ok"):
                        return f"https://telegra.ph/{result['result']['path']}"
                    if "ACCESS_TOKEN" in str(result.get("error", "")).upper():
                        _telegraph_token = None
                        break
        except Exception:
            pass
        if attempt < 2:
            await asyncio.sleep(3)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Helpers — verbatim from original
# ══════════════════════════════════════════════════════════════════════════════

_LANG_MAP = {
    "jpn": "Japanese", "eng": "English", "ger": "German", "deu": "German",
    "spa": "Castilian / Spanish", "fre": "French", "fra": "French",
    "ita": "Italian", "por": "Portuguese", "tha": "Thai", "ara": "Arabic",
    "hin": "Hindi", "chi": "Chinese", "zho": "Chinese", "kor": "Korean",
    "rus": "Russian", "tur": "Turkish", "pol": "Polish", "dut": "Dutch / Flemish",
    "nld": "Dutch / Flemish", "ind": "Indonesian", "may": "Malay",
    "msa": "Malay", "vie": "Vietnamese", "swe": "Swedish", "nor": "Norwegian",
    "dan": "Danish", "fin": "Finnish", "heb": "Hebrew", "ces": "Czech",
    "cze": "Czech", "slk": "Slovak", "hun": "Hungarian", "ron": "Romanian",
    "rum": "Romanian", "bul": "Bulgarian", "hrv": "Croatian", "srp": "Serbian",
    "ukr": "Ukrainian", "cat": "Catalan",
}

def _lang_display(code: str) -> str:
    return _LANG_MAP.get((code or "").lower().strip(), (code or "Unknown").title())

def _humanbytes(size) -> str:
    try:
        size = int(size)
    except (TypeError, ValueError):
        return "N/A"
    if size <= 0:
        return "N/A"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def _fmt_dur_long(seconds: int) -> str:
    h = seconds // 3600; m = (seconds % 3600) // 60; s = seconds % 60
    parts = []
    if h: parts.append(f"{h}hr")
    if m: parts.append(f"{m}mins")
    if s or not parts: parts.append(f"{s}s")
    return " ".join(parts)

def _fmt_timestamp(seconds: float) -> str:
    total = int(seconds); h = total // 3600; m = (total % 3600) // 60; s = total % 60
    return f"{h}:{m:02d}:{s:02d}"

def _fmt_br(br) -> str:
    try:
        br = int(br)
        if br >= 1_000_000: return f"{br / 1_000_000:.2f} Mbps"
        if br >= 1_000:     return f"{br / 1_000:.0f} kbps"
        return f"{br} bps"
    except Exception:
        return str(br)

def _parse_fps(fraction_str: str) -> str:
    try:
        if "/" in fraction_str:
            num, den = fraction_str.split("/")
            val = float(num) / float(den)
            if val <= 0: return ""
            for known in (23.976, 24.0, 25.0, 29.97, 30.0, 48.0, 50.0, 59.94, 60.0, 120.0):
                if abs(val - known) < 0.01:
                    return f"{known:.3f}".rstrip("0").rstrip(".")
            return f"{val:.3f}".rstrip("0").rstrip(".")
        val = float(fraction_str)
        return f"{val:.3f}".rstrip("0").rstrip(".") if val > 0 else ""
    except Exception:
        return ""
