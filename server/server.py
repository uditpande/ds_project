
import socket
import json
import sys
from common.config import BUFFER_SIZE, HELLO,HELLO_REPLY, CHAT,DISCOVER_SERVER,SERVER_INFO


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

    # def listen(self):
    #     while True:
    #         data, addr = self.sock.recvfrom(BUFFER_SIZE)
    #         msg = json.loads(data.decode())
    #         self.handle_message(msg, addr)

    #after sending hello once, server stays alive
    def listen(self):
        self.send_hello()
        while True:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                self.handle_message(msg, addr)
            except ConnectionResetError:
                # handling windows UDP reset — safe to ignore
                continue

    def handle_message(self, msg, addr):
        msg_type = msg.get("type")

        if msg_type == HELLO:
            #prevent duplicate server ids
            if msg["server_id"] == self.server_id:
                print(f"[{self.server_id}] ERROR: Duplicate server ID detected. Ignoring.")
                return

            sender_id = msg["server_id"]
            sender_port = msg["port"]
            # adding to member list
            if sender_id not in self.members:
                self.members[sender_id] = sender_port
                print(f"[{self.server_id}] Discovered server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")

            reply = {
                "type": HELLO_REPLY,
                "server_id": self.server_id,
                "port": self.port
            }
            self.sock.sendto(json.dumps(reply).encode(), addr)

        #completing discovery, new servers learn about existing once and membership converges
        elif msg_type == "HELLO_REPLY":
            sender_id = msg["server_id"]
            sender_port = msg["port"]

            if sender_id not in self.members:
                self.members[sender_id] = sender_port
                print(f"[{self.server_id}] Added server {sender_id}")
                print(f"[{self.server_id}] Members: {self.members}")



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
