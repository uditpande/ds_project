import socket, json

SERVER = ("127.0.0.1", 5001)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(1.0)

msg = {"type": "CLIENT_WHO_IS_LEADER"}
sock.sendto(json.dumps(msg).encode(), SERVER)

data, addr = sock.recvfrom(4096)
print("Reply from", addr, "->", json.loads(data.decode()))
