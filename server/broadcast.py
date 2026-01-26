import json
import socket
from common.config import BUFFER_SIZE, DISCOVERY_PORT

class BroadcastChannel:
    """
    Shared broadcast channel on DISCOVERY_PORT.
    - rx: bound listener (servers receive DISCOVER/HELLO here)
    - tx: sender (broadcast + unicast replies)
    """
    def __init__(self):
        # Receiver socket (servers bind same port)
        self.rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self.rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        self.rx.bind(("", DISCOVERY_PORT))
        self.rx.setblocking(False)

        # Sender socket
        self.tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def try_recv(self):
        """Non-blocking receive; returns (msg_dict, (ip,port)) or (None,None)."""
        try:
            data, addr = self.rx.recvfrom(BUFFER_SIZE)
        except BlockingIOError:
            return None, None
        except OSError:
            return None, None

        try:
            return json.loads(data.decode("utf-8")), addr
        except Exception:
            return None, None

    def send_unicast(self, msg: dict, addr):
        try:
            self.tx.sendto(json.dumps(msg).encode("utf-8"), addr)
        except OSError:
            pass

    def send_broadcast(self, msg: dict):
        try:
            self.tx.sendto(
                json.dumps(msg).encode("utf-8"),
                ("255.255.255.255", DISCOVERY_PORT),
            )
        except OSError:
            pass

    def close(self):
        try: self.rx.close()
        except OSError: pass
        try: self.tx.close()
        except OSError: pass