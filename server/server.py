import socket
import json
import sys
import time
from server.heartbeat import HeartbeatManager
from common.config import (
    BUFFER_SIZE,
    HELLO,
    HELLO_REPLY,
    CHAT,
    DISCOVER_SERVER,
    SERVER_INFO,
    ELECTION,
    ELECTION_OK,
    COORDINATOR,
    HEARTBEAT
)
from server.election import ElectionManager


SERVER_PORTS = [5001, 5002, 5003]  # will replace by broadcast later


class Server:
    def __init__(self, server_id, port):
        self.server_id = server_id
        self.port = port

        # members: server_id -> (ip, port)
        self.members = {}
        # self entry: ip isn't really used (we never send to ourselves), but keep consistent format
        self.members[self.server_id] = ("127.0.0.1", self.port)

        # sockets
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # IMPORTANT for WLAN later: listen on all interfaces, not just localhost
        self.sock.bind(("0.0.0.0", self.port))

        # leader election
        self.leader_id = None
        self.election = ElectionManager(self)
        # Heartbeat
        self.heartbeat = HeartbeatManager(self)

        self.last_election_time = 0.0



        print(f"[{self.server_id}] Server started on port {self.port}")

    # helper: normalize member values (supports old int-only format just in case)
    def member_addr(self, sid):
        v = self.members[sid]
        if isinstance(v, tuple):
            return v  # (ip, port)
        return ("127.0.0.1", v)  # backward-compat

    # discovery initiation
    def send_hello(self):
        hello_msg = {
            "type": HELLO,
            "server_id": self.server_id,
            "port": self.port,
        }
        data = json.dumps(hello_msg).encode()

        # Local testing mode: still ping known local ports on localhost.
        for p in SERVER_PORTS:
            if p != self.port:
                self.sock.sendto(data, ("127.0.0.1", p))

         # ping any already-known members (WLAN-friendly, harmless locally)
        for sid, (ip, port) in self.members.items():
            if sid == self.server_id:
                continue
            try:
                self.sock.sendto(data, (ip, port))
            except OSError:
                # ignore transient send errors
                pass

    def listen(self):
        self.send_hello()
        time.sleep(0.3)

        # start election on startup
        self.election.start_election()

        # allow tick() to run even when no packets arrive
        self.sock.settimeout(0.1)

        while True:
            self.election.tick()
            self.heartbeat.tick()

            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
            except (socket.timeout, TimeoutError):
                continue
            except ConnectionResetError:
                continue

            msg = json.loads(data.decode())
            self.handle_message(msg, addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get("type")

        if msg_type == HELLO:
            # prevent duplicate server ids
            if msg["server_id"] == self.server_id:
                print(f"[{self.server_id}] ERROR: Duplicate server ID detected. Ignoring.")
                return

            sender_id = msg["server_id"]
            sender_port = msg["port"]
            sender_ip = addr[0]  # trust UDP source IP (WLAN-ready)

            if sender_id not in self.members:
                self.members[sender_id] = (sender_ip, sender_port)
                print(f"[{self.server_id}] Discovered server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")
                self.maybe_start_election(newly_seen_id=sender_id)

            reply = {"type": HELLO_REPLY, "server_id": self.server_id, "port": self.port}
            self.sock.sendto(json.dumps(reply).encode(), addr)

        elif msg_type == HELLO_REPLY:
            sender_id = msg["server_id"]
            sender_port = msg["port"]
            sender_ip = addr[0]

            if sender_id not in self.members:
                self.members[sender_id] = (sender_ip, sender_port)
                print(f"[{self.server_id}] Added server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")
                self.maybe_start_election(newly_seen_id=sender_id)

        elif msg_type == CHAT:
            print(f"[{self.server_id}] CHAT from client: {msg['payload']}")

        elif msg_type == DISCOVER_SERVER:
            reply = {"type": SERVER_INFO, "server_id": self.server_id, "port": self.port}
            self.sock.sendto(json.dumps(reply).encode(), addr)

        elif msg_type == ELECTION:
            self.election.on_election(msg, addr)

        elif msg_type == ELECTION_OK:
            self.election.on_election_ok(msg, addr)

        elif msg_type == COORDINATOR:
            self.election.on_coordinator(msg, addr)

        elif msg_type == HEARTBEAT:
            self.heartbeat.on_heartbeat(msg, addr)


        else:
            print(f"[{self.server_id}] Unknown message: {msg}")

    def priority(self, sid: str) -> int:
        return int(sid[1:])

    def set_leader(self, leader_id):
        if self.leader_id == leader_id:
            return
        self.leader_id = leader_id
        print(f"[{self.server_id}] Leader is now {self.leader_id}")
        self.heartbeat.on_leader_changed()

    def maybe_start_election(self, newly_seen_id=None):
        now = time.time()

        # 0.5s cooldown to prevent election storms
        if now - self.last_election_time < 0.5:
            return

        # Don’t start if election already running
        if getattr(self.election, "in_election", False):
            return

        # If we have no leader, elect
        if self.leader_id is None:
            self.last_election_time = now
            self.election.start_election()
            return

        # If we just discovered someone higher than current leader, re-elect
        if newly_seen_id is not None:
            if self.priority(newly_seen_id) > self.priority(self.leader_id):
                self.last_election_time = now
                self.election.start_election()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python server.py <SERVER_ID> <PORT>")
        sys.exit(1)

    server_id = sys.argv[1]
    port = int(sys.argv[2])

    server = Server(server_id, port)
    server.listen()
