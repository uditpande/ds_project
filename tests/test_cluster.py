import json
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

BUFFER_SIZE = 4096

PORTS = [5001, 5002, 5003]
SERVERS = [("S1", 5001), ("S2", 5002), ("S3", 5003)]


def udp_request(sock, addr, msg, timeout=0.4):
    sock.settimeout(timeout)
    sock.sendto(json.dumps(msg).encode(), addr)
    try:
        data, raddr = sock.recvfrom(BUFFER_SIZE)
        return json.loads(data.decode()), raddr
    except (socket.timeout, TimeoutError, ConnectionResetError):
        return None, None


def start_server(project_root: Path, sid: str, port: int):
    # -u = unbuffered stdout (helps if you ever want logs)
    cmd = [sys.executable, "-u", "-m", "server.server", sid, str(port)]
    return subprocess.Popen(
        cmd,
        cwd=str(project_root),
        stdout=subprocess.DEVNULL,   # avoid Windows stdout read blocking
        stderr=subprocess.DEVNULL,
        text=True
    )


def stop_server(proc: subprocess.Popen):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        proc.kill()


def wait_for_cluster_ready(timeout_s=6.0):
    """
    Wait until at least one server answers CLIENT_WHO_IS_LEADER with CLIENT_LEADER_INFO.
    This avoids reading server stdout (Windows-safe).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            for p in PORTS:
                reply, _ = udp_request(sock, ("127.0.0.1", p), {"type": "CLIENT_WHO_IS_LEADER"}, timeout=0.5)
                if reply and reply.get("type") == "CLIENT_LEADER_INFO":
                    return True
            time.sleep(0.1)
        return False
    finally:
        sock.close()


def get_leader_info(sock, any_port):
    reply, _ = udp_request(sock, ("127.0.0.1", any_port), {"type": "CLIENT_WHO_IS_LEADER"}, timeout=1.0)
    assert reply is not None, "No response to CLIENT_WHO_IS_LEADER"
    assert reply.get("type") == "CLIENT_LEADER_INFO"
    assert reply.get("leader_port") in PORTS
    return reply

def send_chat_follow_redirects(sock, port, msg_id, max_hops=5):
    msg = {"type": "CHAT", "sender_id": "TEST", "msg_id": msg_id, "payload": "hi"}
    current_port = port

    for _ in range(max_hops):
        reply, _ = udp_request(sock, ("127.0.0.1", current_port), msg, timeout=1.0)
        if reply is None:
            return None

        if reply.get("type") == "CHAT_ACK" and reply.get("msg_id") == msg_id:
            return reply

        if reply.get("type") == "CLIENT_REDIRECT":
            current_port = reply.get("leader_port")
            continue

        # unexpected packet type
        return reply

    return None



@pytest.fixture(scope="module")
def project_root():
    return Path(__file__).resolve().parents[1]


@pytest.fixture
def servers(project_root):
    procs = []
    try:
        for sid, port in SERVERS:
            procs.append(start_server(project_root, sid, port))

        assert wait_for_cluster_ready(timeout_s=8.0), "Cluster did not become ready in time"
        yield procs
    finally:
        for p in procs:
            stop_server(p)


def test_leader_info(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        info = get_leader_info(sock, any_port=5001)
        assert info["leader_port"] in PORTS
    finally:
        sock.close()

def test_redirect_or_ack_when_chat_sent_to_nonleader_candidate(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        info = get_leader_info(sock, any_port=5002)
        assumed_leader_port = info["leader_port"]

        candidate_ports = [p for p in PORTS if p != assumed_leader_port]
        assert candidate_ports, "Need a candidate non-leader port"

        target_port = candidate_ports[0]
        msg_id = "T-REDIR-1"
        msg = {"type": "CHAT", "sender_id": "TEST", "msg_id": msg_id, "payload": "hi"}

        reply, _ = udp_request(sock, ("127.0.0.1", target_port), msg, timeout=1.0)
        assert reply is not None, "Expected reply but got no reply"

        rtype = reply.get("type")
        assert rtype in ("CLIENT_REDIRECT", "CHAT_ACK")

        if rtype == "CLIENT_REDIRECT":
            assert reply.get("leader_port") in PORTS
            assert reply.get("leader_port") != target_port
        else:  # CHAT_ACK
            assert reply.get("msg_id") == msg_id
    finally:
        sock.close()



def test_ack_from_leader(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        info = get_leader_info(sock, any_port=5001)
        leader_port = info["leader_port"]

        msg_id = "T-ACK-1"
        msg = {"type": "CHAT", "sender_id": "TEST", "msg_id": msg_id, "payload": "hi"}

        reply, _ = udp_request(sock, ("127.0.0.1", leader_port), msg, timeout=1.0)
        assert reply is not None, "Expected CHAT_ACK but got no reply"
        assert reply.get("type") == "CHAT_ACK"
        assert reply.get("msg_id") == msg_id
    finally:
        sock.close()


def test_dedup_ack_twice(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        info = get_leader_info(sock, any_port=5001)
        leader_port = info["leader_port"]

        msg_id = "T-DEDUP-1"
        msg = {"type": "CHAT", "sender_id": "TEST", "msg_id": msg_id, "payload": "hi"}

        r1, _ = udp_request(sock, ("127.0.0.1", leader_port), msg, timeout=1.0)
        r2, _ = udp_request(sock, ("127.0.0.1", leader_port), msg, timeout=1.0)

        assert r1 is not None and r1.get("type") == "CHAT_ACK" and r1.get("msg_id") == msg_id
        assert r2 is not None and r2.get("type") == "CHAT_ACK" and r2.get("msg_id") == msg_id
    finally:
        sock.close()




def test_failover_after_leader_kill(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        info1 = get_leader_info(sock, any_port=5001)
        old_leader_port = info1["leader_port"]

        # find and stop the leader process
        leader_proc = None
        for proc, (sid, port) in zip(servers, SERVERS):
            if port == old_leader_port:
                leader_proc = proc
                break
        assert leader_proc is not None, "Could not locate leader process"
        stop_server(leader_proc)

        # wait until leader changes
        deadline = time.time() + 8.0
        info2 = None
        while time.time() < deadline:
            info2 = get_leader_info(sock, any_port=5002)
            if info2["leader_port"] != old_leader_port:
                break
            time.sleep(0.2)

        assert info2 is not None
        assert info2["leader_port"] in PORTS
        assert info2["leader_port"] != old_leader_port

        # new leader should ACK
        msg_id = "T-FAILOVER-1"
        msg = {"type": "CHAT", "sender_id": "TEST", "msg_id": msg_id, "payload": "hi"}
        ack = send_chat_follow_redirects(sock, info2["leader_port"], msg_id, max_hops=8)
        assert ack is not None, "Did not receive ACK after following redirects"
        assert ack.get("type") == "CHAT_ACK"
        assert ack.get("msg_id") == msg_id

    finally:
        sock.close()

def test_leader_rejoin_still_works(servers, project_root):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    procs = servers  # alias
    try:
        # Find current leader
        info = get_leader_info(sock, any_port=5001)
        leader_port = info["leader_port"]

        # Identify leader process index
        leader_idx = None
        for i, (_, port) in enumerate(SERVERS):
            if port == leader_port:
                leader_idx = i
                break
        assert leader_idx is not None, "Could not locate leader index"

        leader_sid, leader_port = SERVERS[leader_idx]
        leader_proc = procs[leader_idx]

        # Kill leader
        stop_server(leader_proc)


        # Wait for new leader to be different
        deadline = time.time() + 8.0
        new_info = None
        while time.time() < deadline:
            new_info = get_leader_info(sock, any_port=5002)
            if new_info["leader_port"] != leader_port:
                break
            time.sleep(0.2)

        assert new_info is not None
        assert new_info["leader_port"] != leader_port

        # Restart old leader
        procs[leader_idx] = start_server(project_root, leader_sid, leader_port)

        # Allow rejoin + election to settle
        time.sleep(2.0)

        # Chat should still work (redirects handle who is leader now)
        msg_id = "T-REJOIN-1"
        ack = send_chat_follow_redirects(sock, 5001, msg_id, max_hops=12)

        assert ack is not None, "No ACK after leader rejoin"
        assert ack.get("type") == "CHAT_ACK"
        assert ack.get("msg_id") == msg_id
    finally:
        sock.close()


def test_random_kill_then_chat_eventually_acks(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        victim_index = int(time.time()) % len(SERVERS)
        victim_proc = servers[victim_index]
        victim_port = SERVERS[victim_index][1]

        stop_server(victim_proc)

        # Choose a start port that is NOT the killed one (important)
        start_port = next(p for p in PORTS if p != victim_port)

        msg_id = "T-RANDOMKILL-1"

        # Try for up to 10 seconds to get an ACK (election + discovery can take time)
        deadline = time.time() + 10.0
        ack = None
        while time.time() < deadline:
            ack = send_chat_follow_redirects(sock, start_port, msg_id, max_hops=12)
            if ack is not None and ack.get("type") == "CHAT_ACK" and ack.get("msg_id") == msg_id:
                break
            time.sleep(0.3)

        assert ack is not None, "No ACK after random kill (even after retries)"
        assert ack.get("type") == "CHAT_ACK"
        assert ack.get("msg_id") == msg_id
    finally:
        sock.close()



def test_stress_50_acks(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Start by asking any node; leader may change, redirects will handle it.
        start_port = 5001

        for i in range(1, 51):
            msg_id = f"T-STRESS-{i}"
            ack = send_chat_follow_redirects(sock, start_port, msg_id, max_hops=8)
            assert ack is not None, f"Missing ACK for {msg_id}"
            assert ack.get("type") == "CHAT_ACK"
            assert ack.get("msg_id") == msg_id
    finally:
        sock.close()

