import json
import socket
from common.config import BUFFER_SIZE, DISCOVERY_PORT
from common.syslog import LOG_INFO, LOG_WARN

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
        self.rx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self.rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        self.rx.bind(("0.0.0.0", DISCOVERY_PORT))
        self.rx.setblocking(False)

        # Sender socket
        self.tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        LOG_INFO(
            "BROADCAST_INIT",
            server_id="broadcast",
            event="BROADCAST_INIT",
            addr=f"0.0.0.0:{DISCOVERY_PORT}",
        )

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
            LOG_WARN(
                "BROADCAST_RX_JSON_FAIL",
                server_id="broadcast",
                event="BROADCAST_RX_JSON_FAIL",
                addr=f"{addr[0]}:{addr[1]}",
            )
            return None, None

    def send_unicast(self, msg: dict, addr):
        LOG_INFO(
            "BROADCAST_UNICAST_TX",
            server_id="broadcast",
            event="BROADCAST_UNICAST_TX",
            addr=f"{addr[0]}:{addr[1]}",
            msg_type=msg.get("type"),
        )
        try:
            self.tx.sendto(json.dumps(msg).encode("utf-8"), addr)
        except OSError:
            LOG_WARN(
                "BROADCAST_UNICAST_TX_FAIL",
                server_id="broadcast",
                event="BRAODCAST_UNICAST_TX_FAIL",
                addr=f"{addr[0]}:{addr[1]}",
                msg_type=msg.get("type"),
            )
            pass

    def send_broadcast(self, msg: dict):
        LOG_INFO(
            "BROADCAST_BCAST_TX",
            server_id="broadcast",
            event="BROADCAST_BCAST_TX",
            addr=f"255.255.255.255:{DISCOVERY_PORT}",
            msg_type=msg.get("type"),
        )
        try:
            self.tx.sendto(
                json.dumps(msg).encode("utf-8"),
                ("255.255.255.255", DISCOVERY_PORT),
            )
        except OSError:
            LOG_WARN(
                "BROADCAST_BCAST_TX_FAIL",
                server_id="broadcast",
                event="BROADCAST_BCAST_TX_FAIL",
                msg_type=msg.get("type"),
            )
            pass

    def close(self):
        LOG_INFO(
            "BROADCAST_CLOSE",
            server_id="broadcast",
            event="BROADCAST_CLOSE",
        )
        try: self.rx.close()
        except OSError: pass
        try: self.tx.close()
        except OSError: pass