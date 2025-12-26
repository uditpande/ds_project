# import socket
# import json
#
# SERVER_PORT = 5000
# BUFFER_SIZE = 4096
#
# sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# sock.bind(("0.0.0.0", SERVER_PORT))
#
# print(f"Server listening on port {SERVER_PORT}")
#
# while True:
#     data, addr = sock.recvfrom(BUFFER_SIZE)
#     msg = json.loads(data.decode())
#     print("Received:", msg, "from", addr)

import socket
import json
import sys
from common.config import BUFFER_SIZE, HELLO, CHAT,DISCOVER_SERVER,SERVER_INFO

class Server:
    def __init__(self, server_id, port):
        self.server_id = server_id
        self.port = port

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", self.port))

        print(f"[{self.server_id}] Server started on port {self.port}")

    def listen(self):
        while True:
            data, addr = self.sock.recvfrom(BUFFER_SIZE)
            msg = json.loads(data.decode())
            self.handle_message(msg, addr)

    def handle_message(self, msg, addr):
        msg_type = msg.get("type")

        if msg_type == HELLO:
            print(f"[{self.server_id}] HELLO from {msg['sender_id']}")

        elif msg_type == CHAT:
            print(f"[{self.server_id}] CHAT from client: {msg['payload']}")

        elif msg_type == DISCOVER_SERVER:
            reply = {
                "type": SERVER_INFO,
                "server_id": self.server_id,
                "port": self.port
            }
            self.sock.sendto(json.dumps(reply).encode(), addr)

        else:
            print(f"[{self.server_id}] Unknown message: {msg}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python server.py <SERVER_ID> <PORT>")
        sys.exit(1)

    server_id = sys.argv[1]
    port = int(sys.argv[2])

    server = Server(server_id, port)
    server.listen()
