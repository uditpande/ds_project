import socket
import json
import sys
from common.config import BUFFER_SIZE, CHAT, CLIENT_WHO_IS_LEADER, CLIENT_LEADER_INFO
import time
import random

DISCOVERY_PORTS = [5001, 5002, 5003]  # known range

class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def start(self):
        print(f"[{self.client_id}] Client started")

        any_server = self.discover_server()
        self.server_addr = self.get_leader_addr(any_server)

        while True:
            text = input(">> ")
            msg = {
                "type": CHAT,
                "sender_id": self.client_id,
                "payload": text
            }
            self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

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


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <CLIENT_ID>")
        sys.exit(1)

    client_id = sys.argv[1]
    client = Client(client_id)
    client.start()
