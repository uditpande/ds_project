import socket
import json
import sys
import time
import uuid  # NEW

from server.broadcast import BroadcastChannel

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
    HEARTBEAT,
    CLIENT_WHO_IS_LEADER,
    CLIENT_LEADER_INFO,
    CHAT_ACK,
    CLIENT_REGISTERED,
    CLIENT_REGISTER,
    REGISTER_FWD,
    CHAT_FWD,
    CHAT_ACK_FWD,
    CHAT_BCAST,
    CHAT_DELIVER,
)
from server.election import ElectionManager
from server.multicast import MulticastManager


#SERVER_PORTS = [5001, 5002, 5003, 5004, 5005]  # will replace by broadcast later


class Server:
    def __init__(self, server_id, port):
        self.server_id = server_id
        self.port = port

        self.members = {}
        self.members[self.server_id] = ("127.0.0.1", self.port)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.port))
        
        self.leader_id = None
        self.election = ElectionManager(self)
        self.heartbeat = HeartbeatManager(self)

        self.last_election_time = 0.0
        self.hello_interval = 1.0
        self.last_hello_sent = 0.0

        self.seen_dup_sources = set()
        self.seen_chat = {}

        self.clients = {}
        self.seen_register = {}

        self.local_clients = {}          # client_id -> (ip,port)
        self.pending_chat_acks = {}      # msg_id -> client_addr

        # NEW: pending registrations (entry server)
        # req_id -> client_addr (ip,port)
        self.pending_registers = {}

        self.broadcast = BroadcastChannel()
        self.multicast = MulticastManager(self)

        print(f"[{self.server_id}] Server started on port {self.port}")

    def leader_info(self):
        leader_id = self.leader_id if self.leader_id is not None else self.server_id

        if leader_id == self.server_id:
            return {"leader_id": self.server_id, "leader_ip": "127.0.0.1", "leader_port": self.port}

        if leader_id in self.members:
            ip, port = self.member_addr(leader_id)
            return {"leader_id": leader_id, "leader_ip": ip, "leader_port": port}

        return {"leader_id": self.server_id, "leader_ip": "127.0.0.1", "leader_port": self.port}

    def member_addr(self, sid):
        v = self.members[sid]
        if isinstance(v, tuple):
            return v
        return ("127.0.0.1", v)

    def send_hello(self):
        '''hello_msg = {"type": HELLO, "server_id": self.server_id, "port": self.port}
        data = json.dumps(hello_msg).encode()

        for p in SERVER_PORTS:
            if p != self.port:
                self.sock.sendto(data, ("127.0.0.1", p))

        for sid, (ip, port) in self.members.items():
            if sid == self.server_id:
                continue
            try:
                self.sock.sendto(data, (ip, port))
            except OSError:
                pass'''
        # Broadcast hello( server discovery)
        hello_msg = { 
            "type": HELLO,
            "nonce": uuid.uuid4().hex,
            "server_id": self.server_id,
            "ip": "127.0.0.1",
            "port": self.port,
        }
        self.last_hello_nonce = hello_msg["nonce"]
        self.broadcast.send_broadcast(hello_msg)

    def listen(self):
        self.send_hello()
        time.sleep(0.3)
        
        end = time.time() + 0.2
        while time.time() < end:
            bmsg, baddr = self.broadcast.try_recv()
            if not bmsg:
                break
            self.handle_message(bmsg, baddr)

        self.election.start_election()
        self.sock.settimeout(0.1)

        while True:
            self.election.tick()
            self.heartbeat.tick()
            self.multicast.tick()

            now = time.time()
            if now - self.last_hello_sent >= self.hello_interval:
                self.send_hello()
                self.last_hello_sent = now

            # Poll broadcast channel (DISCOVER_SERVER, HELLO) - drain all pending packets
            while True:
                bmsg, baddr = self.broadcast.try_recv()
                if not bmsg:
                    break
                self.handle_message(bmsg, baddr)

            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
            except (socket.timeout, TimeoutError):
                continue
            except ConnectionResetError:
                continue

            msg = json.loads(data.decode())
            self.handle_message(msg, addr)

    def _deliver_to_local_clients(self, from_id: str, payload: str, msg_id: str = None):
        deliver = {"type": CHAT_DELIVER, "from": from_id, "payload": payload}
        if msg_id:
            deliver["msg_id"] = msg_id
        data = json.dumps(deliver).encode()

        for _, caddr in list(self.local_clients.items()):
            try:
                self.sock.sendto(data, caddr)
            except OSError:
                pass

    def _replicate_chat_multicast(self, from_id: str, payload: str, msg_id: str):
        '''bcast = {"type": CHAT_BCAST, "from": from_id, "payload": payload, "msg_id": msg_id}
        data = json.dumps(bcast).encode()

        self._deliver_to_local_clients(from_id, payload, msg_id)

        for sid, (ip, port) in self.members.items():
            if sid == self.server_id:
                continue
            try:
                self.sock.sendto(data, (ip, port))
            except OSError:
                pass'''
        # Leader-sequenced reliable ordered replication via multicast.py
        # IMPORTANT: do NOT deliver locally here; delivery happens via multicast in-order delivery.
        if self.leader_id != self.server_id:
            return

        self.multicast.multicast_chat(from_id, payload, msg_id)

    def handle_message(self, msg, addr):
        if self.multicast.on_message(msg, addr):
            return
        msg_type = msg.get("type")

        if msg_type == HELLO:
            sender_id = msg.get("server_id")
            sender_port = msg.get("port")

            if sender_id is None or sender_port is None:
                return

            # ignore our own broadcast HELLO
            if sender_id == self.server_id:
                return
            
            sender_ip = msg.get("ip") or addr[0]

            if sender_id in self.members and self.members[sender_id] != (sender_ip, sender_port):
                key = (sender_id, sender_ip, sender_port)
                if key not in self.seen_dup_sources:
                    print(
                        f"[{self.server_id}] ERROR: Duplicate server ID '{sender_id}' from "
                        f"{sender_ip}:{sender_port}. Existing is {self.members[sender_id]}. Ignoring."
                    )
                    self.seen_dup_sources.add(key)
                return

            is_new = sender_id not in self.members
            self.members[sender_id] = (sender_ip, sender_port)

            if is_new:
                print(f"[{self.server_id}] Discovered server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")

            self.maybe_start_election(newly_seen_id=sender_id)

            '''reply = {"type": HELLO_REPLY, "server_id": self.server_id, "port": self.port}
            self.sock.sendto(json.dumps(reply).encode(), addr)'''

            #Reply unicast (to sender's broadcast source addr/port)
            reply = {
                "type": HELLO_REPLY,
                "nonce": msg.get("nonce"),
                "server_id": self.server_id,
                "ip":"127.0.0.1",
                "port":self.port,
                "leader_id": self.leader_id if self.leader_id is not None else self.server_id,
            }
            self.broadcast.send_unicast(reply, addr)

        elif msg_type == HELLO_REPLY:
            if msg.get("nonce") != getattr(self, "last_hello_nonce", None):
                return
            sender_id = msg.get("server_id")
            sender_port = msg.get("port")
            if sender_id is None or sender_port is None:
                return
            sender_ip = msg.get("ip") or addr[0]

            if sender_id in self.members and self.members[sender_id] != (sender_ip, sender_port):
                key = (sender_id, sender_ip, sender_port)
                if key not in self.seen_dup_sources:
                    print(
                        f"[{self.server_id}] ERROR: Duplicate server ID '{sender_id}' from "
                        f"{sender_ip}:{sender_port}. Existing is {self.members[sender_id]}. Ignoring."
                    )
                    self.seen_dup_sources.add(key)
                return

            is_new = sender_id not in self.members
            self.members[sender_id] = (sender_ip, sender_port)

            if is_new:
                print(f"[{self.server_id}] Added server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")
            
            self.maybe_start_election(newly_seen_id=sender_id)

        elif msg_type == CLIENT_WHO_IS_LEADER:
            leader_id = self.leader_id if self.leader_id is not None else self.server_id

            if leader_id == self.server_id:
                leader_ip, leader_port = "127.0.0.1", self.port
            else:
                if leader_id in self.members:
                    leader_ip, leader_port = self.member_addr(leader_id)
                else:
                    leader_id = self.server_id
                    leader_ip, leader_port = "127.0.0.1", self.port

            reply = {
                "type": CLIENT_LEADER_INFO,
                "leader_id": leader_id,
                "leader_ip": leader_ip,
                "leader_port": leader_port
            }
            self.sock.sendto(json.dumps(reply).encode(), addr)

        # ----------------- 3B REGISTRATION -----------------
        elif msg_type == CLIENT_REGISTER:
            client_id = (msg.get("client_id") or msg.get("username") or "").strip()
            username = (msg.get("username") or client_id or "anonymous").strip()

            # IMPORTANT: always have a req_id for forwarded registration so we can match replies
            req_id = msg.get("req_id") or uuid.uuid4().hex

            if client_id:
                self.local_clients[client_id] = (addr[0], addr[1])

            # If leader: register and reply directly to client
            if self.leader_id == self.server_id:
                now = time.time()

                cached = self.seen_register.get(req_id)
                if cached is not None:
                    self.sock.sendto(json.dumps(cached).encode(), addr)
                    return

                if not client_id:
                    client_id = f"client-{addr[0]}:{addr[1]}"

                self.clients[client_id] = {
                    "username": username,
                    "registered_at": self.clients.get(client_id, {}).get("registered_at", now),
                    "last_seen": now,
                }

                reply = {
                    "type": CLIENT_REGISTERED,
                    "req_id": req_id,
                    "client_id": client_id,
                    "leader_id": self.leader_id,
                }
                self.seen_register[req_id] = reply
                self.sock.sendto(json.dumps(reply).encode(), addr)
                return

            # Not leader: forward to leader (NO blocking recvfrom here)
            info = self.leader_info()
            leader_addr = (info["leader_ip"], info["leader_port"])

            # remember who to reply to when leader responds
            self.pending_registers[req_id] = (addr[0], addr[1])

            fwd = {
                "type": REGISTER_FWD,
                "req_id": req_id,
                "client_id": client_id,
                "username": username,
                "client_addr": [addr[0], addr[1]],
                "origin_server": self.server_id,
            }
            self.sock.sendto(json.dumps(fwd).encode(), leader_addr)
            return

        elif msg_type == REGISTER_FWD:
            if self.leader_id != self.server_id:
                return

            client_id = (msg.get("client_id") or msg.get("username") or "").strip()
            username = (msg.get("username") or client_id or "anonymous").strip()
            req_id = msg.get("req_id") or uuid.uuid4().hex
            now = time.time()

            cached = self.seen_register.get(req_id)
            if cached is not None:
                self.sock.sendto(json.dumps(cached).encode(), addr)
                return

            if not client_id:
                caddr = msg.get("client_addr") or [addr[0], addr[1]]
                client_id = f"client-{caddr[0]}:{caddr[1]}"

            self.clients[client_id] = {
                "username": username,
                "registered_at": self.clients.get(client_id, {}).get("registered_at", now),
                "last_seen": now,
            }

            reply = {
                "type": CLIENT_REGISTERED,
                "req_id": req_id,
                "client_id": client_id,
                "leader_id": self.leader_id,
            }
            self.seen_register[req_id] = reply

            # reply to forwarding server
            self.sock.sendto(json.dumps(reply).encode(), addr)
            return

        elif msg_type == CLIENT_REGISTERED:
            # NEW: entry server receives leader's reply; relay to the correct client
            req_id = msg.get("req_id")
            if not req_id:
                return

            client_addr = self.pending_registers.pop(req_id, None)
            if client_addr is not None:
                self.sock.sendto(json.dumps(msg).encode(), client_addr)
            return
        # ---------------------------------------------------

        # ----------------- 3B CHAT -----------------
        elif msg_type == CHAT:
            sender = msg.get("sender_id")
            msg_id = msg.get("msg_id")
            payload = msg.get("payload")

            if sender:
                self.local_clients.setdefault(sender, (addr[0], addr[1]))

            if self.leader_id == self.server_id:
                if sender and msg_id:
                    seen = self.seen_chat.setdefault(sender, set())
                    if msg_id in seen:
                        self.sock.sendto(json.dumps({"type": CHAT_ACK, "msg_id": msg_id}).encode(), addr)
                        return
                    seen.add(msg_id)

                print(f"[{self.server_id}] CHAT {msg_id} from {sender}: {payload}")
                self.sock.sendto(json.dumps({"type": CHAT_ACK, "msg_id": msg_id}).encode(), addr)

                if sender and payload is not None and msg_id:
                    self._replicate_chat_multicast(sender, payload, msg_id)
                return

            info = self.leader_info()
            leader_addr = (info["leader_ip"], info["leader_port"])

            if not msg_id:
                return

            self.pending_chat_acks[msg_id] = (addr[0], addr[1])

            fwd = {
                "type": CHAT_FWD,
                "origin_server": self.server_id,
                "sender_id": sender,
                "msg_id": msg_id,
                "payload": payload,
            }
            self.sock.sendto(json.dumps(fwd).encode(), leader_addr)
            return

        elif msg_type == CHAT_FWD:
            if self.leader_id != self.server_id:
                return

            origin = msg.get("origin_server")
            sender = msg.get("sender_id")
            msg_id = msg.get("msg_id")
            payload = msg.get("payload")

            if sender and msg_id:
                seen = self.seen_chat.setdefault(sender, set())
                if msg_id in seen:
                    if origin in self.members:
                        self.sock.sendto(json.dumps({"type": CHAT_ACK_FWD, "msg_id": msg_id}).encode(),
                                         self.member_addr(origin))
                    return
                seen.add(msg_id)

            print(f"[{self.server_id}] CHAT {msg_id} from {sender}: {payload}")

            if origin and origin in self.members:
                self.sock.sendto(json.dumps({"type": CHAT_ACK_FWD, "msg_id": msg_id}).encode(),
                                 self.member_addr(origin))

            if sender and payload is not None and msg_id:
                self._replicate_chat_multicast(sender, payload, msg_id)
            return

        elif msg_type == CHAT_ACK_FWD:
            msg_id = msg.get("msg_id")
            if not msg_id:
                return
            client_addr = self.pending_chat_acks.pop(msg_id, None)
            if client_addr is not None:
                self.sock.sendto(json.dumps({"type": CHAT_ACK, "msg_id": msg_id}).encode(), client_addr)
            return

        elif msg_type == CHAT_BCAST:
            '''from_id = msg.get("from")
            payload = msg.get("payload")
            msg_id = msg.get("msg_id")
            if from_id is not None and payload is not None:
                self._deliver_to_local_clients(from_id, payload, msg_id)'''
            return
        # ---------------------------------------------------

        elif msg_type == DISCOVER_SERVER:
            '''reply = {"type": SERVER_INFO, "server_id": self.server_id, "port": self.port}
            self.sock.sendto(json.dumps(reply).encode(), addr)'''
            # Client broadcast DISCOVER_SERVER -> unicast SERVER_INFO back
            reply = {
                "type": SERVER_INFO,
                "nonce": msg.get("nonce"),
                "server_id": self.server_id,
                "ip": "127.0.0.1",
                "port": self.port,
                "leader_id": self.leader_id if self.leader_id is not None else self.server_id,
                "is_leader": (self.leader_id == self.server_id) if self.leader_id is not None else True,
            }
            self.broadcast.send_unicast(reply, addr)

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

        # reset multicast epoch/seq state on leader change
        self.multicast.on_leader_changed()

        # NEW: followers auto-sync from the leader after leader change
        if self.leader_id != self.server_id:
            self.multicast.request_sync()

    def maybe_start_election(self, newly_seen_id=None):
        now = time.time()

        if now - self.last_election_time < 0.5:
            return

        if getattr(self.election, "in_election", False):
            return

        if self.leader_id is None:
            self.last_election_time = now
            self.election.start_election()
            return

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
