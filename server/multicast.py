"""
server.multicast

Reliable, ordered *application-level multicast* (implemented as repeated unicast)
for server-to-server replication over UDP.

Design:
- Single leader sequences messages
- Leader assigns monotonic sequence numbers (Lamport-style)
- Leader sends MC_DATA to all servers (unicast loop)
- Receivers ACK back to leader
- Leader retransmits until ACKs are received
- Receivers deliver messages in-order using a hold-back queue
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from collections import deque

MC_DATA = "MC_DATA"
MC_ACK = "MC_ACK"
MC_SYNC_REQ = "MC_SYNC_REQ"
MC_SYNC_REPLY = "MC_SYNC_REPLY"


def _now() -> float:
    return time.time()


@dataclass
class _Pending:
    packet: Dict[str, Any]
    acks: Set[str]
    last_sent: float
    retries: int


class MulticastManager:
    def __init__(self, server, retransmit_interval=0.25, max_retries=15):
        self.server = server

        self.epoch = 0
        self.seq = 0

        self._pending: Dict[str, _Pending] = {}
        self._log: List[Dict[str, Any]] = []

        self._holdback: Dict[int, Dict[int, Dict[str, Any]]] = {}
        self._delivered_upto: Dict[int, int] = {}
        self._deliveries: Deque[Dict[str, Any]] = deque()

        self.retransmit_interval = retransmit_interval
        self.max_retries = max_retries

        self._last_sync_req_upto: Dict[int, int] = {}

        self._leader_addr: Optional[Tuple[str, int]] = None
        self.on_leader_changed()

    def is_leader(self) -> bool:
        return self.server.leader_id == self.server.server_id

    def on_leader_changed(self):
        """
        Called whenever Server.set_leader() changes leader_id.
        We must drop any in-flight / buffered state because epochs are leader-owned.
        """
        # Always clear retransmission state
        self._pending.clear()
        self._leader_addr = None

        # Drop ordering state from old leader/epoch
        self._holdback.clear()
        self._delivered_upto.clear()
        self._deliveries.clear()

        # Reset gap-sync guard
        self._last_sync_req_upto.clear()

        if self.is_leader():
            # Start a fresh logical epoch for the new leader term
            self.epoch += 1          # logical epoch bump
            self.seq = 0

            # Leader log is per-epoch; reset so we never replay old leader data
            self._log = []
        else:
            pass

    def tick(self):
        if self.is_leader():
            self._leader_retransmit()
        self._try_deliver()

    def multicast_chat(self, from_id, payload, client_msg_id):
        if not self.is_leader():
            raise RuntimeError("Only leader can multicast")

        self.seq += 1
        msg_key = f"{self.epoch}:{self.seq}:{self.server.server_id}"

        packet = {
            "type": MC_DATA,
            "epoch": self.epoch,
            "seq": self.seq,
            "leader_id": self.server.server_id,
            "msg_key": msg_key,
            "chat": {
                "from": from_id,
                "payload": payload,
                "msg_id": client_msg_id,
            },
        }

        self._log.append(packet)
        self._accept_data(packet, (self.server.my_ip, self.server.port), is_self=True)
        if len(self._log) > 1000:
            self._log.pop(0) #prevents memory growth during long tests
        self._pending[msg_key] = _Pending(packet, set(), 0.0, 0)
        self._send_to_all(packet)
        self._pending[msg_key].last_sent = _now()
        return msg_key

    def on_message(self, msg, addr) -> bool:
        t = msg.get("type")
        if t == MC_DATA:
            self._accept_data(msg, addr)
            return True
        if t == MC_ACK:
            self._on_ack(msg)
            return True
        if t == MC_SYNC_REQ:
            self._on_sync_req(msg, addr)
            return True
        if t == MC_SYNC_REPLY:
            self._on_sync_reply(msg)
            return True
        return False

    def _member_addrs(self):
        addrs = []
        for sid in self.server.members:
            if sid == self.server.server_id:
                continue
            addrs.append(self.server.member_addr(sid))
        return addrs

    def _send_to_all(self, packet):
        data = json.dumps(packet).encode()
        for ip, port in self._member_addrs():
            self.server.sock.sendto(data, (ip, port))

    def _send_ack(self, epoch, msg_key):
        ack = {
            "type": MC_ACK,
            "epoch": epoch,
            "msg_key": msg_key,
            "server_id": self.server.server_id,
        }
        if self._leader_addr:
            self.server.sock.sendto(json.dumps(ack).encode(), self._leader_addr)

    def _accept_data(self, msg, addr, is_self=False):
        epoch = msg["epoch"]
        seq = msg["seq"]

        old_epoch = self.epoch

        # Ignore packets from older epochs (stale leader)
        if epoch < self.epoch:
            if not is_self:
                # Optional: ACK to stop old leader retransmitting
                self._send_ack(epoch, msg["msg_key"])
            return

        # If we see a newer epoch unexpectedly, ignore it for now.
        # The proper epoch update happens via leader election / coordinator.
        if epoch > self.epoch:
            self.epoch = epoch
            self._prune_old_epochs(self.epoch)

            if (epoch > old_epoch) and (not self.is_leader()) and (not is_self):
                self.request_sync()

        # Remember leader address for ACK routing
        if not is_self:
            self._leader_addr = addr

        # Initialize delivery watermark for this epoch
        self._delivered_upto.setdefault(epoch, 0)

        # GAP DETECTION: if we received something beyond the next expected seq,
        # we are missing messages -> ask leader to sync.
        expected = self._delivered_upto[epoch] + 1
        if seq > expected and (not self.is_leader()) and (not is_self):
            last = self._last_sync_req_upto.get(epoch, 0)
            if expected > last:
                self._last_sync_req_upto[epoch] = expected
                self.request_sync()

        # Deduplication: already delivered
        if seq <= self._delivered_upto[epoch]:
            if not is_self:
                self._send_ack(epoch, msg["msg_key"])
            return

        # Buffer for in-order delivery
        self._holdback.setdefault(epoch, {})[seq] = msg

        # ACK receipt (delivery happens later)
        if not is_self:
            self._send_ack(epoch, msg["msg_key"])


    def _on_ack(self, msg):
        if not self.is_leader():
            return
        p = self._pending.get(msg["msg_key"])
        if p:
            p.acks.add(msg["server_id"])


    def _leader_retransmit(self):
        now = _now()
        required = set(self.server.members) - {self.server.server_id}

        for k, p in list(self._pending.items()):
            if required.issubset(p.acks):
                self._pending.pop(k)
                continue
            if now - p.last_sent < self.retransmit_interval:
                continue
            if p.retries >= self.max_retries:
                self._pending.pop(k)
                continue
            self._send_to_all(p.packet)
            p.last_sent = now
            p.retries += 1


    def _try_deliver(self):
        for epoch in sorted(self._holdback):
            nxt = self._delivered_upto[epoch] + 1
            while nxt in self._holdback[epoch]:
                pkt = self._holdback[epoch].pop(nxt)
                self._delivered_upto[epoch] = nxt

                chat = pkt["chat"]
                self.server._deliver_to_local_clients(
                    chat["from"], chat["payload"], chat["msg_id"]
                )
                nxt += 1


    def request_sync(self):
        """
        Ask the current leader for any messages we may have missed.
        Safe to call multiple times.
        """
        if self.is_leader():
            return  # leader doesn't need to sync from itself

        # We need the leader's (ip, port) to send the request.
        # If we haven't received any MC_DATA yet, _leader_addr may be None.
        info = self.server.leader_info()  # already exists in your server.py
        if info["leader_id"] == self.server.server_id and not self.is_leader():
            return  # leader not known yet
        
        leader_addr = (info["leader_ip"], info["leader_port"])

        # Best-effort: use our current epoch watermark if present.
        # If we have never delivered anything, since_seq = 0.
        # NOTE: epoch can differ after leader change; we request using leader's current epoch assumption.
        # If you store epoch in server, pass it; otherwise request with our current epoch value.
        epoch = self.epoch
        since_seq = self._delivered_upto.get(epoch, 0)

        req = {
            "type": MC_SYNC_REQ,
            "epoch": epoch,
            "since_seq": since_seq,
            "server_id": self.server.server_id,
        }
        self.server.sock.sendto(json.dumps(req).encode(), leader_addr)


    def _on_sync_req(self, msg, addr):
        if not self.is_leader():
            return
        epoch = msg["epoch"]
        since = msg["since_seq"]

        # If follower requested wrong/old epoch, sync using leader's current epoch
        if epoch != self.epoch:
            epoch = self.epoch
            since = 0
        msgs = [p for p in self._log if p["epoch"] == epoch and p["seq"] > since]
        reply = {"type": MC_SYNC_REPLY, "epoch": epoch, "msgs": msgs}
        self.server.sock.sendto(json.dumps(reply).encode(), addr)


    def _on_sync_reply(self, msg):
        # Sync reply tells us the leader's epoch; adopt it first
        reply_epoch = msg.get("epoch")
        if reply_epoch is not None and reply_epoch >= self.epoch:
            self.epoch = reply_epoch

        info = self.server.leader_info()
        leader_addr = (info["leader_ip"], info["leader_port"])

        for pkt in msg.get("msgs", []):
            self._accept_data(pkt, leader_addr)


    def _prune_old_epochs(self, keep_epoch: int):
        """Drop holdback/delivery state for epochs older than keep_epoch."""
        for e in list(self._holdback.keys()):
            if e < keep_epoch:
                self._holdback.pop(e, None)
        for e in list(self._delivered_upto.keys()):
            if e < keep_epoch:
                self._delivered_upto.pop(e, None)