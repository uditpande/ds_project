# import socket
# import json
#
# SERVER_ADDR = ("127.0.0.1", 5000)
#
# sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#
# while True:
#     text = input(">> ")
#     msg = {
#         "type": "CHAT",
#         "payload": text
#     }
#     sock.sendto(json.dumps(msg).encode(), SERVER_ADDR)

import socket
import json
import sys
from common.config  import BUFFER_SIZE, CHAT
import time
import random


# SERVER_ADDR = ("127.0.0.1", 5001)  # temporarily
DISCOVERY_PORTS = [5001, 5002, 5003]  # known range


class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def start(self):
        print(f"[{self.client_id}] Client started")
        self.server_addr = self.discover_server()

        while True:
            text = input(">> ")
            msg = {
                "type": CHAT,
                "sender_id": self.client_id,
                "payload": text
            }

            self.sock.sendto(json.dumps(msg).encode(), self.server_addr)

    #initial function, always chose s1 (connection bias int the new function, we added random connection from the replies)
    # def discover_server(self):
    #     discovery_msg = {
    #         "type": "DISCOVER_SERVER",
    #         "sender_id": self.client_id
    #     }
    #
    #     for port in DISCOVERY_PORTS:
    #         self.sock.sendto(
    #             json.dumps(discovery_msg).encode(),
    #             ("127.0.0.1", port)
    #         )
    #
    #     data, addr = self.sock.recvfrom(BUFFER_SIZE)
    #     reply = json.loads(data.decode())
    #     print(f"[{self.client_id}] Connected to {reply['server_id']}")
    #
    #     return ("127.0.0.1", reply["port"])

    def discover_server(self):
        discovery_msg = {
            "type": "DISCOVER_SERVER",
            "sender_id": self.client_id
        }

        # Send discovery to all known server ports
        for port in DISCOVERY_PORTS:
            self.sock.sendto(
                json.dumps(discovery_msg).encode(),
                ("127.0.0.1", port)
            )

        self.sock.settimeout(0.3)  # 300 ms window
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
                # Windows sends this if a port is unreachable (safe to ignore)
                continue

        if not servers:
            raise Exception("No servers found")

        chosen = random.choice(servers)
        print(f"[{self.client_id}] Connected to {chosen['server_id']}")

        return ("127.0.0.1", chosen["port"])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <CLIENT_ID>")
        sys.exit(1)

    client_id = sys.argv[1]
    client = Client(client_id)
    client.start()
