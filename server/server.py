
import socket
import json
import sys
import time
from common.config import BUFFER_SIZE, HELLO,HELLO_REPLY, CHAT,DISCOVER_SERVER,SERVER_INFO,ELECTION, ELECTION_OK, COORDINATOR
from server.election import ElectionManager


SERVER_PORTS = [5001, 5002, 5003]  #will replace by broadcast later

class Server:
    def __init__(self, server_id, port):
        self.server_id = server_id
        self.port = port
        self.members = {}  # server_id -> port
        self.members[self.server_id] = self.port
        #setting up sockets
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", self.port))
        # self.send_hello() now calling inside lister() after socket is bound and listening starts

        # leader election, added by udit after implementing elcection logic in election.py
        self.leader_id = None
        self.election = ElectionManager(self)
        self.last_election_time = 0.0

        print(f"[{self.server_id}] Server started on port {self.port}")


    #discovery initiation, Servers that aren’t running are ignored ,Servers that are running respond
    def send_hello(self):
        hello_msg = {
            "type": HELLO,
            "server_id": self.server_id,
            "port": self.port
        }

        for p in SERVER_PORTS:
            if p != self.port:
                self.sock.sendto(
                    json.dumps(hello_msg).encode(),
                    ("127.0.0.1", p)
                )

    #after sending hello once, server stays alive
    def listen(self):
        self.send_hello()
        time.sleep(0.2)
        # no election call here; discovery triggers election

        while True:
            try:
                #added after implmenting election
                self.election.tick()
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                self.handle_message(msg, addr)
            except ConnectionResetError:
                # handling windows UDP reset — safe to ignore
                continue

    def handle_message(self, msg, addr):
        msg_type = msg.get("type")

        if msg_type == HELLO:
            # prevent duplicate server ids
            if msg["server_id"] == self.server_id:
                print(f"[{self.server_id}] ERROR: Duplicate server ID detected. Ignoring.")
                return

            sender_id = msg["server_id"]
            sender_port = msg["port"]

            if sender_id not in self.members:
                self.members[sender_id] = sender_port
                print(f"[{self.server_id}] Discovered server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")
                self.maybe_start_election(newly_seen_id=sender_id)

            reply = {
                "type": HELLO_REPLY,
                "server_id": self.server_id,
                "port": self.port
            }
            self.sock.sendto(json.dumps(reply).encode(), addr)

        elif msg_type == HELLO_REPLY:
            sender_id = msg["server_id"]
            sender_port = msg["port"]

            if sender_id not in self.members:
                self.members[sender_id] = sender_port
                print(f"[{self.server_id}] Added server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")
                self.maybe_start_election(newly_seen_id=sender_id)

        elif msg_type == CHAT:
            print(f"[{self.server_id}] CHAT from client: {msg['payload']}")

        elif msg_type == DISCOVER_SERVER:
            reply = {
                "type": SERVER_INFO,
                "server_id": self.server_id,
                "port": self.port
            }
            self.sock.sendto(json.dumps(reply).encode(), addr)

        elif msg_type == ELECTION:
            self.election.on_election(msg, addr)

        elif msg_type == ELECTION_OK:
            self.election.on_election_ok(msg, addr)

        elif msg_type == COORDINATOR:
            self.election.on_coordinator(msg, addr)

        else:
            print(f"[{self.server_id}] Unknown message: {msg}")

    # Leader election etc:

    # function to compare server IDs: (added by udit while working on election.py)
    def priority(self, sid: str) -> int:
        return int(sid[1:])

    def set_leader(self, leader_id):
        self.leader_id = leader_id
        print(f"[{self.server_id}] Leader is now {self.leader_id}")

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
            self.election.start_election()
            return

        # If we just discovered someone higher than current leader, re-elect
        if newly_seen_id is not None:
            if self.priority(newly_seen_id) > self.priority(self.leader_id):
                self.election.start_election()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python server.py <SERVER_ID> <PORT>")
        sys.exit(1)

    server_id = sys.argv[1]
    port = int(sys.argv[2])

    server = Server(server_id, port)
    server.listen()
