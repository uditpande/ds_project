import socket
import json
import sys
import time
import random
import uuid
import threading
import queue

from common.config import (
    BUFFER_SIZE,
    CHAT,
    CHAT_ACK,
    CLIENT_REGISTER,
    CLIENT_REGISTERED,
    CHAT_DELIVER,
    DISCOVER_SERVER,
    SERVER_INFO,
    DISCOVERY_PORT,
    BROADCAST_ADDR,
    BIND_ADDR,
)

#DISCOVERY_PORTS = [5001, 5002, 5003]  # known range


class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((BIND_ADDR, 0))
        self.seq = 0
        self.session_id = uuid.uuid4().hex[:6]
        self.server_addr = None
        self.registered_id = None

        # NEW: inbox for non-CHAT_DELIVER messages (ACKs, redirects, register replies)
        self.inbox = queue.Queue()

    def flush_socket(self):
        self.sock.settimeout(0.0)
        while True:
            try:
                self.sock.recvfrom(BUFFER_SIZE)
            except (BlockingIOError, socket.timeout, ConnectionResetError):
                break

        # also clear any queued inbox messages
        while True:
            try:
                self.inbox.get_nowait()
            except queue.Empty:
                break

    def recv_loop(self):
        # SINGLE reader of the UDP socket:
        # - prints CHAT_DELIVER immediately
        # - queues everything else into inbox for send_chat/register to consume
        self.sock.settimeout(0.2)
        while True:
            try:
                data, _ = self.sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())

                if msg.get("type") == CHAT_DELIVER:
                    sender = msg.get("from")
                    if sender == (self.registered_id or self.client_id):
                        continue  # don't show my own broadcast
                    print(f"\n[{sender}] {msg.get('payload')}\n>> ", end="")
                else:
                    self.inbox.put(msg)


            except (socket.timeout, BlockingIOError, ConnectionResetError):
                continue

    def discover_server(self):
        # Enable broadcast on this socket (safe to call multiple times)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        nonce = uuid.uuid4().hex
        discovery_msg = {
            "type": DISCOVER_SERVER,
            "nonce": nonce,
            "client_id": self.client_id,   # (server may ignore; fine)
        }

        # Broadcast once to DISCOVERY_PORT
        self.sock.sendto(json.dumps(discovery_msg).encode(), (BROADCAST_ADDR, DISCOVERY_PORT))

        # NOTE: discover_server runs before recv_loop starts, so it can safely recvfrom itself.
        self.sock.settimeout(0.1)
        servers = {}

        deadline = time.time() + 0.8
        while time.time() < deadline:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue

            try:
                reply = json.loads(data.decode())
            except Exception:
                continue

            if reply.get("type") != SERVER_INFO:
                continue
            if reply.get("nonce") != nonce:
                continue

            sid = reply.get("server_id")
            port = reply.get("port")

            if sid is None or port is None:
                continue
            
            # Fallback to the sender's IP (addr[0]) instead of 127.0.0.1
            ip = reply.get("ip") or addr[0]

            servers[(sid, ip, int(port))] = reply

        if not servers:
            raise Exception("No servers found")
        
        # Prefer leader if present
        unique_servers = list(servers.values())

        leaders = [s for s in unique_servers if s.get("is_leader")]
        chosen = random.choice(leaders) if leaders else random.choice(unique_servers)
        ip = chosen.get("ip")
        port = chosen.get("port")

        if ip is None or port is None:
            (sid, ip, port) = next(iter(servers.keys()))
        else:
            port = int(port)

        print(
            f"[{self.client_id}] Discovered server {chosen.get('server_id')} "
            f"at {ip}:{port} "
            f"(leader={chosen.get('is_leader')}, leader_id={chosen.get('leader_id')})"
        )
        
        return (ip, port)

    def register(self):
        msg = {
            "type": CLIENT_REGISTER,
            "client_id": self.client_id,
            "username": self.client_id,
        }

        # send to ENTRY server (3B design)
        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        deadline = time.time() + 0.8
        while time.time() < deadline:
            try:
                reply = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue

            rtype = reply.get("type")

            if rtype == CLIENT_REGISTERED:
                self.registered_id = reply.get("client_id") or self.client_id
                print(f"[{self.client_id}] Registered successfully as {self.registered_id}")
                return

            # ignore unrelated messages
        print(f"[{self.client_id}] Registration timed out")

    def send_chat(self, text):
        self.seq += 1

        sender = self.registered_id or self.client_id
        msg_id = f"{sender}-{self.session_id}-{self.seq}"

        msg = {
            "type": CHAT,
            "sender_id": sender,
            "msg_id": msg_id,
            "payload": text
        }

        # send once to current ENTRY server
        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        deadline = time.time() + 0.6
        while time.time() < deadline:
            try:
                reply = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue

            rtype = reply.get("type")

            if rtype == CHAT_ACK and reply.get("msg_id") == msg_id:
                print(f"[{self.client_id}] got ACK for {msg_id}")
                return

            # ignore unrelated messages

        # If we got here, the entry server is likely unreachable
        print(f"[{self.client_id}] Server unreachable, rediscovering entry server...")
        self.flush_socket()
        self.server_addr = self.discover_server()
        print(f"[{self.client_id}] New entry server {self.server_addr}")
        self.register()

        # retry once
        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        deadline = time.time() + 0.6
        while time.time() < deadline:
            try:
                reply = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue

            if reply.get("type") == CHAT_ACK and reply.get("msg_id") == msg_id:
                print(f"[{self.client_id}] got ACK for {msg_id} after rediscovery")
                return

    def start(self):
        print(f"[{self.client_id}] Client started")

        # Entry server (3B): stick to it
        self.server_addr = self.discover_server()
        print(f"[{self.client_id}] Using entry server {self.server_addr}")

        self.flush_socket()

        # start receiver thread AFTER discovery (so discovery recvfrom isn't contested)
        t = threading.Thread(target=self.recv_loop, daemon=True)
        t.start()

        # register once (now uses inbox instead of recvfrom)
        self.register()

        while True:
            text = input(">> ")
            self.send_chat(text)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m client.client <CLIENT_ID>")
        sys.exit(1)

    client_id = sys.argv[1]
    client = Client(client_id)
    client.start()
