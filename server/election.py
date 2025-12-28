import json
import time
from common.config import ELECTION, ELECTION_OK, COORDINATOR


def server_priority(server_id: str) -> int:
    return int(server_id[1:])


class ElectionManager:
    def __init__(self, server):
        """
        server is our Server instance from server.py
        We use it for:
          - server.server_id
          - server.members
          - server.sock.sendto(...)
          - server.set_leader(...)
        """
        self.server = server
        self.in_election = False
        self.got_ok = False
        self.waiting_ok = False
        self.ok_deadline = 0.0

    def start_election(self):
        if self.in_election:
            return

        self.in_election = True
        self.got_ok = False
        self.waiting_ok = False

        my_pri = server_priority(self.server.server_id)

        higher = []
        for sid, port in self.server.members.items():
            if sid == self.server.server_id:
                continue
            if server_priority(sid) > my_pri:
                higher.append((sid, port))

        if not higher:
            # nobody higher -> the current server becomes the  leader
            self.become_leader()
            return

        # send ELECTION to higher nodes
        msg = {"type": ELECTION, "server_id": self.server.server_id}
        data = json.dumps(msg).encode()

        for sid, port in higher:
            self.server.sock.sendto(data, ("127.0.0.1", port))

        # wait for OK
        self.waiting_ok = True
        self.ok_deadline = time.time() + 1.0  # 1 second window, experiment with it later

    # def tick(self):
    #     """Called periodically from server loop to handle timeouts."""
    #     if self.waiting_ok and time.time() > self.ok_deadline:
    #         # Coordinator did not arrive in time
    #         self.waiting_ok = False
    #         self.in_election = False
    #         # Restart election to recover
    #         self.start_election()

    def tick(self):
        if self.waiting_ok and time.time() > self.ok_deadline:
            self.waiting_ok = False

            if not self.got_ok:
                # No higher node responded -> I can become leader
                self.in_election = False
                self.become_leader()
            else:
                # Higher node DID respond, but coordinator didn't arrive
                # Restart election to recover, but do NOT self-declare yet
                self.in_election = False
                self.start_election()

    def on_election(self, msg, addr):
        """Handle incoming ELECTION message."""
        sender_id = msg["server_id"]

        # Reply OK if I'm higher
        if server_priority(self.server.server_id) > server_priority(sender_id):
            ok = {"type": ELECTION_OK, "server_id": self.server.server_id}
            self.server.sock.sendto(json.dumps(ok).encode(), addr)

            # and start my own election (bully takeover)
            self.start_election()

    def on_election_ok(self, msg, addr):
        """Someone higher exists, so I should not declare myself leader."""
        if self.in_election:
            self.got_ok = True
            # Someone higher exists, so we wait for COORDINATOR
            self.waiting_ok = True
            self.ok_deadline = time.time() + 2.0  # wait longer for coordinator

    def on_coordinator(self, msg, addr):
        leader_id = msg["server_id"]
        self.in_election = False
        self.waiting_ok = False
        self.server.set_leader(leader_id)

    def become_leader(self):
        self.in_election = False
        self.waiting_ok = False
        leader_id = self.server.server_id
        self.server.set_leader(leader_id)

        # broadcast COORDINATOR to all
        coord = {"type": COORDINATOR, "server_id": leader_id}
        data = json.dumps(coord).encode()
        for sid, port in self.server.members.items():
            if sid == self.server.server_id:
                continue
            self.server.sock.sendto(data, ("127.0.0.1", port))
