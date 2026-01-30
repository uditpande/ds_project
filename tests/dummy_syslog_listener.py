import socket

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", 5514))

print("Dummy syslog listener on UDP 5514 (Ctrl+C to stop)")

while True:
    data, addr = s.recvfrom(65535)