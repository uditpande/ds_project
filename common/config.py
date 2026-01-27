BUFFER_SIZE = 65535
DISCOVERY_PORT = 40000

BIND_ADDR = "0.0.0.0"
BROADCAST_ADDR = "255.255.255.255"

# Message types
HELLO = "HELLO"
HELLO_REPLY = "HELLO_REPLY"
CHAT = "CHAT"
DISCOVER_SERVER = "DISCOVER_SERVER"
SERVER_INFO = "SERVER_INFO"

# Leader Election
ELECTION = "ELECTION"
ELECTION_OK = "ELECTION_OK"
COORDINATOR = "COORDINATOR"
HEARTBEAT = "HEARTBEAT"

# Client / leader discovery
CLIENT_WHO_IS_LEADER = "CLIENT_WHO_IS_LEADER"
CLIENT_LEADER_INFO = "CLIENT_LEADER_INFO"
CLIENT_REDIRECT = "CLIENT_REDIRECT"

# Client registration
CLIENT_REGISTER = "CLIENT_REGISTER"
CLIENT_REGISTERED = "CLIENT_REGISTERED"
REGISTER_FWD = "REGISTER_FWD"

# Chat flow
CHAT_ACK = "CHAT_ACK"
CHAT_FWD = "CHAT_FWD"
CHAT_ACK_FWD = "CHAT_ACK_FWD"
CHAT_BCAST = "CHAT_BCAST"
CHAT_DELIVER = "CHAT_DELIVER"

# ==============================
# Syslog / Wireshark observability
# ==============================

SYSLOG_ENABLED = True

# IP address of the machine running Wireshark
# If Wireshark is on the same machine, keep 127.0.0.1
SYSLOG_HOST = "127.0.0.1"

# UDP port used only for syslog-style event packets
SYSLOG_PORT = 5514

# Syslog metadata (for RFC-style header)
SYSLOG_FACILITY = 16   # local0
SYSLOG_SEVERITY = 6    # info
