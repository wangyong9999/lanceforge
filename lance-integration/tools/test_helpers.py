"""
Shared test utilities for LanceForge E2E tests.

Provides:
- find_free_port() — dynamic port allocation (no hardcoded conflicts)
- wait_for_grpc() — readiness check instead of sleep()
- wait_for_process() — verify process didn't crash
"""

import socket
import time
import subprocess
import grpc


def find_free_port() -> int:
    """Bind to port 0 to get a random free port from the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def wait_for_grpc(host: str, port: int, timeout: float = 15.0) -> bool:
    """Poll gRPC endpoint until it responds or timeout expires.

    Returns True if endpoint became available, False on timeout.
    """
    deadline = time.time() + timeout
    addr = f"{host}:{port}"
    while time.time() < deadline:
        try:
            channel = grpc.insecure_channel(addr)
            future = grpc.channel_ready_future(channel)
            future.result(timeout=1.0)
            channel.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def wait_for_grpc_tls(host: str, port: int, ca_cert_path: str, timeout: float = 15.0) -> bool:
    """Poll gRPC TLS endpoint until it responds or timeout expires."""
    deadline = time.time() + timeout
    addr = f"{host}:{port}"
    with open(ca_cert_path, 'rb') as f:
        ca_pem = f.read()
    while time.time() < deadline:
        try:
            creds = grpc.ssl_channel_credentials(root_certificates=ca_pem)
            channel = grpc.secure_channel(addr, creds)
            future = grpc.channel_ready_future(channel)
            future.result(timeout=1.0)
            channel.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def wait_for_process(proc: subprocess.Popen, timeout: float = 3.0) -> bool:
    """Wait briefly and check that process is still running (didn't crash).

    Returns True if process is alive, False if it exited.
    """
    try:
        proc.wait(timeout=timeout)
        return False  # process exited = bad
    except subprocess.TimeoutExpired:
        return True  # still running = good
