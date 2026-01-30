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
    CLIENT_PONG,
    CLIENT_PING,
)

from common.syslog import LOG_INFO, LOG_WARN, LOG_ERROR


class Client:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((BIND_ADDR, 0))

        self.seq = 0
        self.session_id = uuid.uuid4().hex[:6]
        self.server_addr = None
        self.registered_id = None

        # Inbox for non-CHAT_DELIVER messages (ACKs, register replies, etc.)
        self.inbox = queue.Queue()

        # Keepalive / liveness
        self.last_seen = time.time()
        self.keepalive_interval = 1.0
        self.dead_after = 3.0

        # Prevent double reconnects
        self.conn_lock = threading.Lock()
        self.reconnecting = False

        LOG_INFO(
            "CLIENT_STARTED",
            server_id=self.client_id,   # reuse server_id field for node id
            event="CLIENT_STARTED",
        )

    def flush_socket(self):
        self.sock.settimeout(0.0)
        while True:
            try:
                self.sock.recvfrom(BUFFER_SIZE)
            except (BlockingIOError, socket.timeout, ConnectionResetError):
                break

        while True:
            try:
                self.inbox.get_nowait()
            except queue.Empty:
                break

    def recv_loop(self):
        """
        Single reader of self.sock:
        - Prints CHAT_DELIVER immediately (without re-printing prompt, to avoid corrupting input)
        - Queues everything else into inbox
        - Updates liveness ONLY for packets from current server_addr
        """
        self.sock.settimeout(0.2)
        while True:
            try:
                data, raddr = self.sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                mtype = msg.get("type")

                # Liveness: only refresh if message is from current server_addr
                if self.server_addr and raddr == self.server_addr:
                    self.last_seen = time.time()

                # Keepalive response: do not spam inbox
                if mtype == CLIENT_PONG:
                    LOG_INFO(
                        "CLIENT_PONG_RECV",
                        server_id=self.client_id,
                        event="CLIENT_PONG_RECV",
                        addr=f"{raddr[0]}:{raddr[1]}",
                    )
                    continue

                if mtype == CHAT_DELIVER:
                    sender = msg.get("from")
                    if sender == (self.registered_id or self.client_id):
                        continue

                    LOG_INFO(
                        "CLIENT_CHAT_DELIVER",
                        server_id=self.client_id,
                        event="CLIENT_CHAT_DELIVER",
                        from_id=sender,
                        msg_id=msg.get("msg_id"),
                    )

                    print(f"\n[{sender}] {msg.get('payload')}")

                else:
                    # For control messages, only accept those from current entry server
                    if self.server_addr and raddr != self.server_addr:
                        LOG_WARN(
                            "CL_CTRL_FROM_OTHER_SERVER_IGNORED",
                            server_id=self.client_id,
                            event="CL_CTRL_FROM_OTHER_SERVER_IGNORED",
                            addr=f"{raddr[0]}:{raddr[1]}",
                            expected=f"{self.server_addr[0]}:{self.server_addr[1]}",
                            msg_type=mtype,
                        )
                        continue
                    self.inbox.put(msg)

            except (socket.timeout, BlockingIOError, ConnectionResetError):
                continue
            except Exception:
                continue

    def discover_server(self):
        """
        Uses a temporary socket so recv_loop on self.sock doesn't steal replies.
        """
        dsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dsock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        dsock.bind((BIND_ADDR, 0))

        nonce = uuid.uuid4().hex
        discovery_msg = {
            "type": DISCOVER_SERVER,
            "nonce": nonce,
            "client_id": self.client_id,
        }

        LOG_INFO(
            "CLIENT_DISCOVER_SENT",
            server_id=self.client_id,
            event="CLIENT_DISCOVER_SENT",
            nonce=nonce,
            addr=f"{BROADCAST_ADDR}:{DISCOVERY_PORT}",
        )

        dsock.sendto(json.dumps(discovery_msg).encode(), (BROADCAST_ADDR, DISCOVERY_PORT))

        dsock.settimeout(0.1)
        servers = {}

        deadline = time.time() + 0.8
        while time.time() < deadline:
            try:
                data, addr = dsock.recvfrom(BUFFER_SIZE)
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

            ip = reply.get("ip") or addr[0]
            servers[(sid, ip, int(port))] = reply

            LOG_INFO(
                "CLIENT_SERVER_INFO_RECV",
                server_id=self.client_id,
                event="CLIENT_SERVER_INFO_RECV",
                peer_id=sid,
                leader_id=reply.get("leader_id"),
                is_leader=reply.get("is_leader"),
                addr=f"{ip}:{int(port)}",
            )

        dsock.close()

        if not servers:
            LOG_ERROR(
                "CLIENT_DISCOVER_FAIL",
                server_id=self.client_id,
                event="CLIENT_DISCOVER_FAIL",
                nonce=nonce,
            )
            raise Exception("No servers found")

        chosen = random.choice(list(servers.values()))
        ip = chosen.get("ip")
        port = int(chosen.get("port"))

        LOG_INFO(
            "CLIENT_ENTRY_CHOSEN",
            server_id=self.client_id,
            event="CLIENT_ENTRY_CHOSEN",
            peer_id=chosen.get("server_id"),
            addr=f"{ip}:{port}",
            leader_id=chosen.get("leader_id"),
            entry_is_leader=chosen.get("is_leader"),
        )

        print(
            f"[{self.client_id}] Entry server {chosen.get('server_id')} at {ip}:{port} "
            f"(cluster_leader_id={chosen.get('leader_id')}, entry_is_leader={chosen.get('is_leader')})"
        )
        return (ip, port)

    def register(self) -> bool:
        if not self.server_addr:
            LOG_WARN(
                "CLIENT_REGISTER_NO_SERVER",
                server_id=self.client_id,
                event="CLIENT_REGISTER_NO_SERVER",
            )
            print(f"[{self.client_id}] Cannot register: no server selected")
            return False

        req_id = uuid.uuid4().hex
        msg = {
            "type": CLIENT_REGISTER,
            "client_id": self.client_id,
            "username": self.client_id,
        }

        LOG_INFO(
            "CLIENT_REGISTER_SENT",
            server_id=self.client_id,
            event="CLIENT_REGISTER_SENT",
            req_id=req_id,
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        deadline = time.time() + 1.5
        while time.time() < deadline:
            try:
                reply = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue

            if reply.get("type") == CLIENT_REGISTERED:
                self.registered_id = reply.get("client_id") or self.client_id
                LOG_INFO(
                    "CLIENT_REGISTERED_OK",
                    server_id=self.client_id,
                    event="CLIENT_REGISTERED_OK",
                    req_id=req_id,
                    leader_id=reply.get("leader_id"),
                    client_id=self.registered_id,
                )

                print(f"[{self.client_id}] Registered successfully as {self.registered_id}")
                return True

        LOG_WARN(
            "CLIENT_REGISTER_TIMEOUT",
            server_id=self.client_id,
            event="CLIENT_REGISTER_TIMEOUT",
            req_id=req_id,
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

        print(f"[{self.client_id}] Registration timed out")
        return False

    def _wait_reconnect_done(self, timeout=2.0) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            with self.conn_lock:
                busy = self.reconnecting
            if not busy:
                return True
            time.sleep(0.05)
        return False

    def send_chat(self, text: str):
        if not text:
            return

        self._wait_reconnect_done(timeout=2.0)

        if not self.server_addr:
            self._reconnect("no server on send")
            self._wait_reconnect_done(timeout=2.0)
            if not self.server_addr:
                return

        self.seq += 1
        sender = self.registered_id or self.client_id
        msg_id = f"{sender}-{self.session_id}-{self.seq}"

        msg = {
            "type": CHAT,
            "sender_id": sender,
            "msg_id": msg_id,
            "payload": text,
        }

        LOG_INFO(
            "CLIENT_CHAT_SENT",
            server_id=self.client_id,
            event="CLIENT_CHAT_SENT",
            msg_id=msg_id,
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        deadline = time.time() + 1.2
        while time.time() < deadline:
            try:
                reply = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue

            if reply.get("type") == CHAT_ACK and reply.get("msg_id") == msg_id:
                LOG_INFO(
                    "CLIENT_CHAT_ACK_RECEIVED",
                    server_id=self.client_id,
                    event="CLIENT_CHAT_ACK_RECEIVED",
                    msg_id=msg_id,
                )
                return
            
        LOG_WARN(
            "CLIENT_CHAT_ACK_TIMEOUT",
            server_id=self.client_id,
            event="CLIENT_CHAT_ACK_TIMEOUT",
            msg_id=msg_id,
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

        self._reconnect("chat ack timeout")
        self._wait_reconnect_done(timeout=2.0)
        if not self.server_addr:
            return
        
        LOG_INFO(
            "CLIENT_CHAT_RETRY",
            server_id=self.client_id,
            event="CLIENT_CHAT_RETRY",
            msg_id=msg_id,
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        deadline = time.time() + 1.2
        while time.time() < deadline:
            try:
                reply = self.inbox.get(timeout=0.2)
            except queue.Empty:
                continue

            if reply.get("type") == CHAT_ACK and reply.get("msg_id") == msg_id:
                LOG_INFO(
                    "CLIENT_CHAT_ACK_RECEIVED",
                    server_id=self.client_id,
                    event="CLIENT_CHAT_ACK_RECEIVED",
                    msg_id=msg_id,
                )
                return
            
        LOG_ERROR(
            "CLIENT_CHAT_FAIL_NO_ACK",
            server_id=self.client_id,
            event="CLIENT_CHAT_FAIL_NO_ACK",
            msg_id=msg_id,
        )

        print(f"[{self.client_id}] Failed to deliver message after reconnect (no ACK)")

    def _send_ping(self):
        if not self.server_addr:
            return
        ping = {"type": CLIENT_PING, "client_id": (self.registered_id or self.client_id)}
        try:
            self.sock.sendto(json.dumps(ping).encode(), self.server_addr)
        except OSError:
            pass

        LOG_INFO(
            "CLIENT_PING_SENT",
            server_id=self.client_id,
            event="CLIENT_PING_SENT",
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

    def _reconnect(self, reason: str):
        with self.conn_lock:
            if self.reconnecting:
                return
            self.reconnecting = True

        try:
            LOG_WARN(
                "CLIENT_RECONNECT_START",
                server_id=self.client_id,
                event="CLIENT_RECONNECT_START",
                reason=reason,
            )
            print(f"[{self.client_id}] Reconnecting ({reason})...")
            self.flush_socket()

            try:
                self.server_addr = self.discover_server()
            except Exception as e:
                LOG_ERROR(
                    "CLIENT_RECONNECT_DISCOVERY_FAIL",
                    server_id=self.client_id,
                    event="CLIENT_RECONNECT_DISCOVERY_FAIL",
                    reason=str(e),
                )
                print(f"[{self.client_id}] Discovery failed: {e}")
                self.server_addr = None
                return

            LOG_INFO(
                "CLIENT_RECONNECT_NEW_ENTRY",
                server_id=self.client_id,
                event="CLIENT_RECONNECT_NEW_ENTRY",
                addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
            )
            print(f"[{self.client_id}] New entry server {self.server_addr}")

            ok = self.register()
            if not ok:
                LOG_ERROR(
                    "CLIENT_RECONNECT_REGISTER_FAIL",
                    server_id=self.client_id,
                    event="CLIENT_RECONNECT_REGISTER_FAIL",
                )
                self.server_addr = None
                return

            self.last_seen = time.time()

            LOG_INFO(
                "CLIENT_RECONNECT_DONE",
                server_id=self.client_id,
                event="CLIENT_RECONNECT_DONE",
                addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
            )

        finally:
            with self.conn_lock:
                self.reconnecting = False

    def keepalive_loop(self):
        while True:
            time.sleep(self.keepalive_interval)

            if not self.server_addr:
                continue

            with self.conn_lock:
                if self.reconnecting:
                    continue

            self._send_ping()

            if (time.time() - self.last_seen) > self.dead_after:
                LOG_WARN(
                    "CLIENT_KEEPALIVE_TIMEOUT",
                    server_id=self.client_id,
                    event="CLIENT_KEEPALIVE_TIMEOUT",
                    addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
                    dead_after=self.dead_after,
                )
                self._reconnect("keepalive timeout")

    def start(self):
        LOG_INFO(
            "CLIENT_START",
            server_id=self.client_id,
            event="CLIENT_START",
        )
        print(f"[{self.client_id}] Client started")

        self.server_addr = self.discover_server()

        LOG_INFO(
            "CLIENT_USE_ENTRY",
            server_id=self.client_id,
            event="CLIENT_USE_ENTRY",
            addr=f"{self.server_addr[0]}:{self.server_addr[1]}",
        )

        print(f"[{self.client_id}] Using entry server {self.server_addr}")

        self.flush_socket()

        t = threading.Thread(target=self.recv_loop, daemon=True)
        t.start()

        self.register()

        ka = threading.Thread(target=self.keepalive_loop, daemon=True)
        ka.start()

        # interactive loop (this was missing in your pasted file)
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
