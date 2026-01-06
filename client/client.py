import socket
import json
import sys
from common.config import BUFFER_SIZE, CHAT, CLIENT_WHO_IS_LEADER, CLIENT_LEADER_INFO, CLIENT_REDIRECT, CHAT_ACK
import time
import random
import uuid


DISCOVERY_PORTS = [5001, 5002, 5003]  # known range

class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.seq = 0
        self.session_id = uuid.uuid4().hex[:6]

    def flush_socket(self):
        self.sock.settimeout(0.0)
        while True:
            try:
                self.sock.recvfrom(BUFFER_SIZE)
            except (BlockingIOError, socket.timeout, ConnectionResetError):
                break


    def start(self):
        print(f"[{self.client_id}] Client started")

        any_server = self.discover_server()
        self.server_addr = self.get_leader_addr(any_server)

        self.flush_socket()

        while True:
            text = input(">> ")
            self.send_chat(text)

    def get_leader_addr(self, any_server_addr):
        msg = {"type": CLIENT_WHO_IS_LEADER}
        self.sock.settimeout(0.5)

        try:
            self.sock.sendto(json.dumps(msg).encode(), any_server_addr)
            data, _ = self.sock.recvfrom(BUFFER_SIZE)
            reply = json.loads(data.decode())

            if reply.get("type") != CLIENT_LEADER_INFO:
                raise Exception(f"Unexpected reply: {reply}")

            leader_ip = reply["leader_ip"]
            leader_port = reply["leader_port"]
            leader_id = reply["leader_id"]

            print(f"[{self.client_id}] Leader is {leader_id} at {leader_ip}:{leader_port}")
            return (leader_ip, leader_port)

        except socket.timeout:
            raise Exception("Leader query timed out")

    def discover_server(self):
        discovery_msg = {
            "type": "DISCOVER_SERVER",
            "sender_id": self.client_id
        }

        for port in DISCOVERY_PORTS:
            self.sock.sendto(json.dumps(discovery_msg).encode(), ("127.0.0.1", port))

        self.sock.settimeout(0.3)
        servers = []

        start = time.time()
        while time.time() - start < 0.3:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                reply = json.loads(data.decode())
                if reply["type"] == "SERVER_INFO":
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

    def send_chat(self, text):
        self.seq += 1
        msg_id = f"{self.client_id}-{self.session_id}-{self.seq}"

        msg = {
            "type": CHAT,
            "sender_id": self.client_id,
            "msg_id": msg_id,
            "payload": text
        }

        # send once to current leader addr
        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

        # wait briefly for either ACK or REDIRECT
        self.sock.settimeout(0.5)
        end = time.time() + 0.5

        while time.time() < end:
            try:
                data, _ = self.sock.recvfrom(BUFFER_SIZE)
                reply = json.loads(data.decode())

                rtype = reply.get("type")

                if rtype == CLIENT_REDIRECT:
                    self.server_addr = (reply["leader_ip"], reply["leader_port"])
                    print(f"[{self.client_id}] Redirected to leader {self.server_addr}")

                    # resend once to redirected leader
                    self.sock.sendto(json.dumps(msg).encode(), self.server_addr)
                    # keep looping to wait for ACK
                    continue

                if rtype == CHAT_ACK and reply.get("msg_id") == msg_id:
                    print(f"[{self.client_id}] got ACK for {msg_id}")
                    return



                # ignore unrelated packets
            except socket.timeout:
                return
            except ConnectionResetError:
                # Windows UDP: happens if destination port is closed (leader died)
                break

        # If we got here, leader likely died → rediscover and retry once
        print(f"[{self.client_id}] Leader unreachable, rediscovering...")
        any_server = self.discover_server()
        self.server_addr = self.get_leader_addr(any_server)
        # self.sock.sendto(json.dumps(msg).encode(), self.server_addr)
        self.sock.sendto(json.dumps(msg).encode(), self.server_addr)
        # (optional) wait a bit for ACK; if none, just return
        self.sock.settimeout(0.5)
        try:
            data, _ = self.sock.recvfrom(BUFFER_SIZE)
            reply = json.loads(data.decode())
            if reply.get("type") == CHAT_ACK and reply.get("msg_id") == msg_id:
                print(f"[{self.client_id}] got ACK for {msg_id} after rediscovery")
        except (socket.timeout, ConnectionResetError):
            pass


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <CLIENT_ID>")
        sys.exit(1)

    client_id = sys.argv[1]
    client = Client(client_id)
    client.start()
