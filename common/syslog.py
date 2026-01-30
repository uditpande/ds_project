# common/syslog.py
import socket
from datetime import datetime, timezone

from common.config import (
    SYSLOG_ENABLED,
    SYSLOG_HOST,
    SYSLOG_PORT,
    SYSLOG_FACILITY,
)

# ------------------------------
# UDP socket (reused)
# ------------------------------
_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


# ------------------------------
# Helpers
# ------------------------------
def _ts():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _get_lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def _pri(severity: int):
    # PRI = facility * 8 + severity
    return (SYSLOG_FACILITY * 8) + severity


def _fmt(v):
    if v is None:
        return "-"
    return str(v)


# ------------------------------
# Core syslog sender
# ------------------------------
def _send_syslog(
    *,
    level: str,
    severity: int,
    message: str,
    server_id: str,
    event = None,
    epoch=None,
    seq=None,
    msg_id=None,
    leader_id=None,
    addr=None,
    **extra
):
    if not SYSLOG_ENABLED:
        return

    host = SYSLOG_HOST
    if host == "auto":
        host = _get_lan_ip()

    if isinstance(addr, tuple) and len(addr) == 2:
        addr = f"{addr[0]}:{addr[1]}"

    # -------- SYSLOG MESSAGE (this goes to Info column) --------
    # This is the SIP-style LOG(INFO)("...") text
    payload_parts = [
        f"event={_fmt(event)}" if event else "event=-",
        f"level={level}",
        f"server_id={server_id}",
        f'msg="{message}"',
    ]

    # -------- Structured fields (still filterable) --------
    structured = {
        "event": event,
        "epoch": epoch,
        "seq": seq,
        "msg_id": msg_id,
        "leader_id": leader_id,
        "addr": addr,
    }

    for k, v in structured.items():
        if v is not None:
            payload_parts.append(f"{k}={_fmt(v)}")

    for k in sorted(extra.keys()):
        payload_parts.append(f"{k}={_fmt(extra[k])}")

    payload = " ".join(payload_parts)

    # RFC5424 header
    syslog_msg = (
        f"<{_pri(severity)}>1 "
        f"{_ts()} "
        f"{server_id} "
        f"ds-chat "
        f"- - - "
        f"{payload}"
    )

    try:
        _sock.sendto(
            syslog_msg.encode("utf-8", errors="replace"),
            (host, SYSLOG_PORT),
        )
    except OSError:
        pass


# ------------------------------
# SIP-STYLE PUBLIC API
# ------------------------------
def LOG_INFO(message: str, **fields):
    _send_syslog(
        level="INFO",
        severity=6,
        message=message,
        **fields,
    )


def LOG_WARN(message: str, **fields):
    _send_syslog(
        level="WARN",
        severity=4,
        message=message,
        **fields,
    )


def LOG_ERROR(message: str, **fields):
    _send_syslog(
        level="ERROR",
        severity=3,
        message=message,
        **fields,
    )