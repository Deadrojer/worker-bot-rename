"""
shared/protocol.py
══════════════════════════════════════════════════════════════════════════════
Wire protocol for Worker Control Group messages.

Every message posted to the Worker Control Group is a plain-text JSON object.
The first field is always "type", which selects the message kind.

Message types (string constants below):

  Manager → Group (read by Workers via String Session):
    MSG_TASK        — assign a job to a specific worker

  Worker → Group (read by Manager via String Session):
    MSG_REGISTER    — worker announces itself
    MSG_HEARTBEAT   — periodic liveness ping
    MSG_ACK         — worker accepted the task
    MSG_STATE       — worker reporting a job state transition
    MSG_RESULT      — worker finished and uploaded the file
    MSG_FAILED      — worker could not complete the job

Job states (persisted in MongoDB):
    QUEUED ASSIGNED ACKED DOWNLOADING PROCESSING UPLOADING
    UPLOADED DELIVERING DELIVERED DONE FAILED

Protocol version: 1
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# ── Protocol version ──────────────────────────────────────────────────────────
PROTOCOL_VERSION = 1

# ── Message type constants ────────────────────────────────────────────────────
MSG_TASK      = "TASK"
MSG_REGISTER  = "REGISTER"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_ACK       = "ACK"
MSG_STATE     = "STATE"
MSG_RESULT    = "RESULT"
MSG_FAILED    = "FAILED"

# ── Job state constants ───────────────────────────────────────────────────────
STATE_QUEUED      = "QUEUED"
STATE_ASSIGNED    = "ASSIGNED"
STATE_ACKED       = "ACKED"
STATE_DOWNLOADING = "DOWNLOADING"
STATE_PROCESSING  = "PROCESSING"
STATE_UPLOADING   = "UPLOADING"
STATE_UPLOADED    = "UPLOADED"
STATE_DELIVERING  = "DELIVERING"
STATE_DELIVERED   = "DELIVERED"
STATE_DONE        = "DONE"
STATE_FAILED      = "FAILED"

# States that are terminal — no further transitions allowed
TERMINAL_STATES = {STATE_DONE, STATE_FAILED}

# Ordered list for validation
VALID_STATES = [
    STATE_QUEUED, STATE_ASSIGNED, STATE_ACKED,
    STATE_DOWNLOADING, STATE_PROCESSING, STATE_UPLOADING,
    STATE_UPLOADED, STATE_DELIVERING, STATE_DELIVERED,
    STATE_DONE, STATE_FAILED,
]

# ── Worker status constants ───────────────────────────────────────────────────
WORKER_ONLINE     = "ONLINE"
WORKER_BUSY       = "BUSY"
WORKER_FLOOD_WAIT = "FLOOD_WAIT"
WORKER_OFFLINE    = "OFFLINE"

# ── Marker prefix (allows fast filtering of non-protocol messages) ────────────
# Every protocol message starts with this prefix so the String Session client
# can skip unrelated group messages cheaply.
PROTO_PREFIX = "⚙️PROTO:"


# ══════════════════════════════════════════════════════════════════════════════
# Encode / Decode
# ══════════════════════════════════════════════════════════════════════════════

def encode(msg: dict) -> str:
    """Serialize a protocol dict to a wire string ready to post in the group."""
    msg.setdefault("protocol_version", PROTOCOL_VERSION)
    msg.setdefault("ts", int(time.time()))
    return PROTO_PREFIX + json.dumps(msg, ensure_ascii=False)


def decode(text: str) -> dict | None:
    """
    Deserialize a wire string.
    Returns None if the message is not a protocol message or is malformed.
    Never raises.
    """
    if not text or not text.startswith(PROTO_PREFIX):
        return None
    payload = text[len(PROTO_PREFIX):]
    try:
        obj = json.loads(payload)
        if not isinstance(obj, dict) or "type" not in obj:
            return None
        return obj
    except Exception as exc:
        logger.debug("[proto] decode failed: %s — text=%r", exc, text[:120])
        return None


def is_proto(text: str) -> bool:
    """Fast check: is this text a protocol message at all?"""
    return bool(text) and text.startswith(PROTO_PREFIX)


# ══════════════════════════════════════════════════════════════════════════════
# Message builders  (Manager side — sends TASK)
# ══════════════════════════════════════════════════════════════════════════════

def make_task(
    worker_id:         str,
    job_id:            str,
    batch_id:          str,
    user_id:           int,
    source_chat_id:    int,
    source_message_id: int,
    rename_pattern:    str,
    prefix:            str,
    suffix:            str,
    metadata:          dict,
    metadata_version:  int = 1,  # default to 1 — version 0 caused metadata to be skipped
    thumbnail_url:     str | None = None,
    dump_enabled:      bool = False,
) -> str:
    """Build a plain-text TASK proto string sent to the Worker Control Group.
    The worker fetches the source file itself via get_messages(source_chat_id,
    source_message_id) using its own BOT_TOKEN client."""
    return encode({
        "type":               MSG_TASK,
        "worker_id":          worker_id,
        "job_id":             job_id,
        "batch_id":           batch_id,
        "user_id":            user_id,
        "source_chat_id":     source_chat_id,
        "source_message_id":  source_message_id,
        "rename_pattern":     rename_pattern,
        "prefix":             prefix,
        "suffix":             suffix,
        "metadata":           metadata,
        "metadata_version":   metadata_version,
        "thumbnail_url":      thumbnail_url,
        "dump_enabled":       dump_enabled,
    })



# ══════════════════════════════════════════════════════════════════════════════
# Message builders  (Worker side — sends REGISTER/HEARTBEAT/ACK/STATE/RESULT/FAILED)
# ══════════════════════════════════════════════════════════════════════════════

def make_register(
    worker_id:  str,
    bot_id:     int,
    username:   str,
    capacity:   int,
    version:    str = "1.0.0",
) -> str:
    return encode({
        "type":             MSG_REGISTER,
        "worker_id":        worker_id,
        "bot_id":           bot_id,
        "username":         username,
        "capacity":         capacity,
        "version":          version,
    })


def make_heartbeat(
    worker_id:   str,
    bot_id:      int,
    active_jobs: int,
    capacity:    int,
    status:      str = WORKER_ONLINE,
) -> str:
    return encode({
        "type":        MSG_HEARTBEAT,
        "worker_id":   worker_id,
        "bot_id":      bot_id,
        "active_jobs": active_jobs,
        "capacity":    capacity,
        "status":      status,
    })


def make_ack(worker_id: str, job_id: str) -> str:
    return encode({
        "type":      MSG_ACK,
        "worker_id": worker_id,
        "job_id":    job_id,
    })


def make_state(worker_id: str, job_id: str, state: str) -> str:
    return encode({
        "type":      MSG_STATE,
        "worker_id": worker_id,
        "job_id":    job_id,
        "state":     state,
    })


def make_result(
    worker_id:         str,
    job_id:            str,
    output_chat_id:    int,
    output_message_id: int,
    filename:          str,
    original_filename: str,
    file_size:         int,
    mediainfo_url:     str | None = None,
) -> str:
    return encode({
        "type":              MSG_RESULT,
        "worker_id":         worker_id,
        "job_id":            job_id,
        "output_chat_id":    output_chat_id,
        "output_message_id": output_message_id,
        "filename":          filename,
        "original_filename": original_filename,
        "file_size":         file_size,
        "mediainfo_url":     mediainfo_url,
    })


def make_failed(
    worker_id: str,
    job_id:    str,
    reason:    str,
) -> str:
    return encode({
        "type":      MSG_FAILED,
        "worker_id": worker_id,
        "job_id":    job_id,
        "reason":    reason,
    })


# ══════════════════════════════════════════════════════════════════════════════
# Validation helpers
# ══════════════════════════════════════════════════════════════════════════════

def require_fields(msg: dict, *fields: str) -> list[str]:
    """Return list of fields missing from msg. Empty list = all present."""
    return [f for f in fields if f not in msg]
