import json
import time
from common.config import HEARTBEAT
from common.syslog import LOG_INFO, LOG_WARN


class HeartbeatManager:
    def __init__(self, server, interval=0.5, timeout=1.5):
        """
        server must provide:
          - server.server_id
          - server.leader_id
          - server.members  (server_id -> (ip, port))
          - server.sock.sendto(...)
          - server.election.start_election()
          - server.set_leader(...) (already exists)
          - server.last_election_time (used for cooldown)
        """
        self.server = server
        self.interval = interval
        self.timeout = timeout

        self.last_sent = 0.0
        self.last_from_leader = time.time()

    def is_leader(self) -> bool:
        return self.server.leader_id == self.server.server_id

    def tick(self):
        now = time.time()

        # Leader: send heartbeat periodically
        if self.is_leader():
            if now - self.last_sent >= self.interval:
                msg = {"type": HEARTBEAT, "server_id": self.server.server_id}
                data = json.dumps(msg).encode()

                for sid, (ip, port) in self.server.members.items():
                    if sid == self.server.server_id:
                        continue
                    try:
                        self.server.sock.sendto(data, (ip, port))
                    except OSError:
                        pass

                self.last_sent = now
                LOG_INFO(
                    "HEARTBEAT_SEND",
                    server_id=self.server.server_id,
                    event="HEARTBEAT_SEND",
                    leader_id=self.server.server_id,
                    interval=self.interval,
                )
            return

        # Follower: check if leader is alive
        if self.server.leader_id is None:
            return

        # Don't interfere during an election
        if getattr(self.server.election, "in_election", False):
            return

        if now - self.last_from_leader > self.timeout:
            # Cooldown to avoid storms
            if now - getattr(self.server, "last_election_time", 0.0) < 0.5:
                return

            # Suspect leader failed
            self.server.last_election_time = now
            self.server.leader_id = None
            self.server.multicast.on_leader_changed()

            LOG_WARN(
                "HEARTBEAT_TIMEOUT",
                server_id=self.server.server_id,
                event="HEARTBEAT_TIMEOUT",
                leader_id=self.server.leader_id,
                timeout=self.timeout,
            )
            
            print(f"[{self.server.server_id}] Leader heartbeat timeout. Starting election...")
            self.server.election.start_election()

    def on_heartbeat(self, msg, addr):
        sender_id = msg.get("server_id")
        if sender_id is None:
            return

        # Accept heartbeats only from the leader we currently believe in
        if sender_id == self.server.leader_id:
            LOG_INFO(
                "HEARTBEAT_RECV",
                server_id=self.server.server_id,
                event="HEARTBEAT_RECV",
                leader_id=sender_id,
                addr=f"{addr[0]}:{addr[1]}",
            )
            self.last_from_leader = time.time()

    def on_leader_changed(self):
        """Call this when server.set_leader() changes leader."""
        now = time.time()
        self.last_from_leader = now
        self.last_sent = 0.0
