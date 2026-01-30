import json
import time
from common.config import ELECTION, ELECTION_OK, COORDINATOR
from common.syslog import LOG_WARN, LOG_INFO


def server_priority(server_id: str) -> int:
    return int(server_id[1:])


class ElectionManager:
    def __init__(self, server):
        """
        server is our Server instance from server.py
        We use it for:
          - server.server_id
          - server.members  (now: server_id -> (ip, port))
          - server.sock.sendto(...)
          - server.set_leader(...)
        """
        self.server = server
        self.in_election = False
        self.got_ok = False
        self.waiting_ok = False
        self.ok_deadline = 0.0

        # MINIMAL ADD: separate wait for COORDINATOR (avoids endless restarts)
        self.waiting_coord = False
        self.coord_deadline = 0.0

    def start_election(self):
        if self.in_election:
            return
        
        LOG_WARN(
            "ELECTION_START",
            server_id=self.server.server_id,
            event="ELECTION_START",
            members_count=len(self.server.members),
        )

        self.in_election = True
        self.got_ok = False
        self.waiting_ok = False
        self.waiting_coord = False

        my_pri = server_priority(self.server.server_id)

        higher = []
        for sid, addr in self.server.members.items():
            if sid == self.server.server_id:
                continue

            ip, port = addr
            if server_priority(sid) > my_pri:
                higher.append((sid, ip, port))

        if not higher:
            # nobody higher -> the current server becomes the leader
            LOG_INFO(
                "ELECTION_NO_HIGHER",
                server_id=self.server.server_id,
                event="ELECTION_NO_HIGHER",
            )
            self.become_leader()
            return

        # send ELECTION to higher nodes
        msg = {"type": ELECTION, "server_id": self.server.server_id}
        data = json.dumps(msg).encode()

        for sid, ip, port in higher:
            LOG_INFO(
                "ELECTION_SEND",
                server_id=self.server.server_id,
                event="ELECTION_SEND",
                peer_id=sid,
                addr=f"{ip}:{port}",
            )
            self.server.sock.sendto(data, (ip, port))

        # wait for OK (phase 1)
        self.waiting_ok = True
        self.ok_deadline = time.time() + 1.0  # OK window

    def tick(self):
        now = time.time()

        # Phase 1: waiting for OKs from higher nodes
        if self.in_election and self.waiting_ok and now > self.ok_deadline:
            self.waiting_ok = False

            if not self.got_ok:
                # No higher node responded -> I can become leader
                self.become_leader()
                return

            # Higher node DID respond -> now wait for COORDINATOR (phase 2)
            self.waiting_coord = True
            self.coord_deadline = now + 2.0  # coordinator window

        # Phase 2: waiting for COORDINATOR announcement
        if self.in_election and self.waiting_coord and now > self.coord_deadline:
            # Coordinator never arrived -> restart election
            self.waiting_coord = False
            self.in_election = False
            self.start_election()

    def on_election(self, msg, addr):
        """Handle incoming ELECTION message."""
        sender_id = msg["server_id"]

        # Reply OK if I'm higher
        if server_priority(self.server.server_id) > server_priority(sender_id):
            LOG_INFO(
                "ELECTION_OK_SENT",
                server_id=self.server.server_id,
                event="ELECTION_OK_SENT",
                peer_id=sender_id,
                addr=f"{addr[0]}:{addr[1]}",
            )
            ok = {"type": ELECTION_OK, "server_id": self.server.server_id}
            self.server.sock.sendto(json.dumps(ok).encode(), addr)

            # and start my own election (bully takeover)
            self.start_election()

    def on_election_ok(self, msg, addr):
        """Someone higher exists, so I should not declare myself leader."""
        if not self.in_election:
            return

        LOG_INFO(
            "ELECTION_OK_RECEIVED",
            server_id=self.server.server_id,
            event="ELECTION_OK_RECEIVED",
            from_id=msg.get("server_id"),
            addr=f"{addr[0]}:{addr[1]}",
        )
        self.got_ok = True
        # IMPORTANT: do NOT extend deadlines here; just record got_ok.
        # We keep waiting until ok_deadline and then move to waiting_coord.

    def on_coordinator(self, msg, addr):
        leader_id = msg["server_id"]

        self.in_election = False
        self.waiting_ok = False
        self.got_ok = False
        self.waiting_coord = False

        LOG_INFO(
            "ELECTION_COORDINATOR_RECEIVED",
            server_id=self.server.server_id,
            event="ELECTION_COORDINATOR_RECEIVED",
            leader_id=leader_id,
            addr=f"{addr[0]}:{addr[1]}",
        )

        self.server.set_leader(leader_id)

    def become_leader(self):

        leader_id = self.server.server_id

        LOG_INFO(
            "ELECTION_WIN",
            server_id=self.server.server_id,
            event="ELECTION_WIN",
            leader_id=leader_id,
        )

        self.in_election = False
        self.waiting_ok = False
        self.got_ok = False
        self.waiting_coord = False

        self.server.set_leader(leader_id)

        # broadcast COORDINATOR to all
        coord = {"type": COORDINATOR, "server_id": leader_id}
        data = json.dumps(coord).encode()

        for sid, addr in self.server.members.items():
            if sid == self.server.server_id:
                continue
            ip, port = addr
            LOG_INFO(
                "ELECTION_COORDINATOR_SENT",
                server_id=self.server.server_id,
                event="ELECTION_COORDINATOR_SENT",
                peer_id=sid,
                addr=f"{ip}:{port}",
            )

            self.server.sock.sendto(data, (ip, port))
