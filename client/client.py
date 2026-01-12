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
    CLIENT_REDIRECT,
    CHAT_ACK,
    CLIENT_REGISTER,
    CLIENT_REGISTERED,
    CHAT_DELIVER,
    DISCOVER_SERVER,
    SERVER_INFO,
)

DISCOVERY_PORTS = [5001, 5002, 5003]  # known range


class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
        discovery_msg = {
            "type": DISCOVER_SERVER,
            "sender_id": self.client_id
        }

        for port in DISCOVERY_PORTS:
            self.sock.sendto(json.dumps(discovery_msg).encode(), ("127.0.0.1", port))

        # NOTE: discover_server runs before recv_loop starts, so it can safely recvfrom itself.
        self.sock.settimeout(0.3)
        servers = []

        start = time.time()
        while time.time() - start < 0.3:
            try:
                data, _ = self.sock.recvfrom(BUFFER_SIZE)
                reply = json.loads(data.decode())
                if reply.get("type") == SERVER_INFO:
                    servers.append(reply)
            except socket.timeout:
                break
            except ConnectionResetError:
                continue

        if not servers:
            raise Exception("No servers found")

        chosen = random.choice(servers)
        print(f"[{self.client_id}] Discovered server {chosen['server_id']} on port {chosen['port']}")
        return ("127.0.0.1", chosen["port"])

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

            if rtype == CLIENT_REDIRECT:
                # transitional behavior: server still may redirect to leader
                self.server_addr = (reply["leader_ip"], reply["leader_port"])
                print(f"[{self.client_id}] Redirected during register to {self.server_addr}")
                self.sock.sendto(json.dumps(msg).encode(), self.server_addr)
                continue

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

            if rtype == CLIENT_REDIRECT:
                # transitional behavior: server still may redirect to leader
                self.server_addr = (reply["leader_ip"], reply["leader_port"])
                print(f"[{self.client_id}] Redirected to {self.server_addr}")
                self.sock.sendto(json.dumps(msg).encode(), self.server_addr)
                continue

            if rtype == CHAT_ACK and reply.get("msg_id") == msg_id:
                print(f"[{self.client_id}] got ACK for {msg_id}")
                return

            # ignore unrelated messages

        # If we got here, the entry server is likely unreachable
        print(f"[{self.client_id}] Server unreachable, rediscovering entry server...")
        self.server_addr = self.discover_server()
        print(f"[{self.client_id}] New entry server {self.server_addr}")

        self.flush_socket()
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
