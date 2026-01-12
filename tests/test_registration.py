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
    cmd = [sys.executable, "-u", "-m", "server.server", sid, str(port)]
    return subprocess.Popen(
        cmd,
        cwd=str(project_root),
        stdout=subprocess.DEVNULL,
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


def send_register_follow_redirects(sock, start_port, req_id, username="udit", client_id=None, max_hops=8):
    msg = {"type": "CLIENT_REGISTER", "req_id": req_id, "username": username, "client_id": client_id}
    current_port = start_port

    for _ in range(max_hops):
        reply, _ = udp_request(sock, ("127.0.0.1", current_port), msg, timeout=1.0)
        if reply is None:
            return None

        if reply.get("type") == "CLIENT_REGISTERED" and reply.get("req_id") == req_id:
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


# -------------------- STEP 1: basic registration --------------------

def test_register_basic_returns_client_id(servers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Start anywhere; redirects should lead us to the leader.
        rep = send_register_follow_redirects(sock, start_port=5001, req_id="R-BASIC-1", username="udit", max_hops=12)

        assert rep is not None, "No reply / did not converge to CLIENT_REGISTERED"
        assert rep.get("type") == "CLIENT_REGISTERED"
        assert rep.get("req_id") == "R-BASIC-1"
        assert isinstance(rep.get("client_id"), str) and rep["client_id"], f"Missing client_id: {rep}"
        assert rep.get("leader_port") in PORTS, f"Missing/invalid leader_port: {rep}"
    finally:
        sock.close()
