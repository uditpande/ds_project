# common/syslog.py
import socket
from datetime import datetime, timezone

from common.config import (
    SYSLOG_ENABLED,
    SYSLOG_HOST,
    SYSLOG_PORT,
    SYSLOG_FACILITY,
    SYSLOG_SEVERITY,
)

_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def _ts():
    """UTC timestamp (ISO-8601, ms)"""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _pri():
    """Syslog PRI value"""
    return (SYSLOG_FACILITY * 8) + SYSLOG_SEVERITY


def _fmt(v):
    """Consistent field formatting"""
    if v is None:
        return "-"
    return str(v)


# ==============================
# PUBLIC API
# ==============================

def syslog_event(
    *,
    server_id,
    event,
    epoch=None,
    seq=None,
    msg_id=None,
    leader_id=None,
    addr=None,
    **extra
):
    """
    Send one syslog-style UDP packet describing a system event.

    Required:
      server_id, event

    Standard fields:
      epoch, seq, msg_id, leader_id, addr
    """
    if not SYSLOG_ENABLED:
        return

    # Normalize addr if tuple
    if isinstance(addr, tuple) and len(addr) == 2:
        addr = f"{addr[0]}:{addr[1]}"

    fields = {
        "server_id": server_id,
        "event": event,
        "epoch": epoch,
        "seq": seq,
        "msg_id": msg_id,
        "leader_id": leader_id,
        "addr": addr,
    }

    # Build logfmt payload (stable order)
    payload_parts = []
    for key in [
        "server_id",
        "event",
        "epoch",
        "seq",
        "msg_id",
        "leader_id",
        "addr",
    ]:
        payload_parts.append(f"{key}={_fmt(fields[key])}")

    # Extra fields (sorted)
    for key in sorted(extra.keys()):
        payload_parts.append(f"{key}={_fmt(extra[key])}")

    payload = " ".join(payload_parts)

    # RFC5424-like header (simplified)
    msg = (
        f"<{_pri()}>1 "
        f"{_ts()} "
        f"{server_id} "
        f"ds-chat "
        f"- - - "
        f"{payload}"
    )

    try:
        _sock.sendto(
            msg.encode("utf-8", errors="replace"),
            (SYSLOG_HOST, SYSLOG_PORT),
        )
    except OSError:
        # Logging must never break the system
        pass