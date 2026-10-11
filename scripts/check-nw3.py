"""Exercise real NW3 managed processes, pipe-to-SSE logs and cumulative IPC statistics.

Run: python3 scripts/check-nw3.py OPENCTRL_BINARY NOWHERE_BINARY
Uses isolated process state, loopback listeners and the Python standard library.
"""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.request

KEY = "0123456789abcdef0123456789abcdef"
TIMEOUT = 15


def wait_for(predicate):
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("integration deadline elapsed")


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def exact(connection, size):
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        assert chunk, "unexpected EOF"
        data.extend(chunk)
    return bytes(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("openctrl")
    parser.add_argument("nowhere")
    args = parser.parse_args()
    nowhere = str(Path(args.nowhere).resolve())
    events = []
    errors = []
    instances = []
    with tempfile.TemporaryDirectory(prefix="nw3-", dir="/tmp") as directory:
        root = Path(directory)
        master_binary = root / "openctrl"
        shutil.copy2(Path(args.openctrl).resolve(), master_binary)
        api_port = port()
        output = root / "master.log"
        env = dict(os.environ, TMPDIR=directory, TOKIO_WORKER_THREADS="2")
        with output.open("wb") as log:
            master = subprocess.Popen([str(master_binary),
                f"master://127.0.0.1:{api_port}?bin={nowhere}"], env=env,
                stdout=log, stderr=log, stdin=subprocess.DEVNULL)
        try:
            token = wait_for(lambda: re.search(r"API key created: (\S+)", output.read_text()))[1]
            base = f"http://127.0.0.1:{api_port}/api/v2"

            def request(path, method="GET", body=None):
                data = None if body is None else json.dumps(body).encode()
                req = urllib.request.Request(base + path, data=data, method=method,
                    headers={"X-API-Key": token, "Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
                    return json.load(response)

            def consume():
                try:
                    req = urllib.request.Request(base + "/events", headers={"X-API-Key": token})
                    with urllib.request.urlopen(req, timeout=60) as response:
                        for line in response:
                            if line.startswith(b"data: "):
                                events.append(json.loads(line[6:]))
                except Exception as error:
                    if master.poll() is None:
                        errors.append(type(error).__name__)

            observer = threading.Thread(target=consume, daemon=True)
            observer.start()
            wait_for(lambda: events)

            def create(url):
                value = request("/instances", "POST", {"url": url})
                identity = value["id"]
                instances.append(identity)
                request("/instances/" + identity, "PATCH", {"restart": False})
                return identity

            def get(identity):
                return request("/instances/" + identity)

            def action(identity, value):
                return request("/instances/" + identity, "PATCH", {"action": value})

            def logs(identity):
                return [event["logs"] for event in events if event["type"] == "log"
                        and event.get("instance", {}).get("id") == identity]

            portal_port = port()
            portal = create(f"portal://{KEY}@127.0.0.1:{portal_port}?out=egress")
            fingerprint = wait_for(lambda: next((re.search(r"fingerprint: ([0-9a-f]{64})", text)
                for text in logs(portal) if "fingerprint:" in text), None))[1]
            egress = create(f"egress://{KEY}@127.0.0.1:{portal_port}?pin={fingerprint}")
            ingress = port()
            vector = create(f"vector://{KEY}@127.0.0.1:{portal_port}"
                f"?in=127.0.0.1:{ingress}&net=tcp&pin={fingerprint}")
            for identity in (portal, egress, vector):
                wait_for(lambda: any("starting:" in text for text in logs(identity)))
                assert get(identity)["status"] == "running"
            time.sleep(1)
            payload = b"nw3-openctrl-known-bytes" * 128

            def transfer():
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    listener.listen(1)
                    target = listener.getsockname()[1]

                    def echo():
                        with listener.accept()[0] as connection:
                            while data := connection.recv(65536):
                                connection.sendall(data)

                    worker = threading.Thread(target=echo, daemon=True)
                    worker.start()
                    with socket.create_connection(("127.0.0.1", ingress), TIMEOUT) as flow:
                        flow.settimeout(TIMEOUT)
                        flow.sendall(b"\x05\x01\x00")
                        assert exact(flow, 2) == b"\x05\x00"
                        flow.sendall(b"\x05\x01\x00\x01\x7f\x00\x00\x01" + struct.pack("!H", target))
                        assert exact(flow, 10)[:2] == b"\x05\x00"
                        flow.sendall(payload)
                        assert exact(flow, len(payload)) == payload
                        for identity in (portal, egress, vector):
                            wait_for(lambda: get(identity)["tcps"] == 1)
                        flow.shutdown(socket.SHUT_WR)
                        assert flow.recv(1) == b""
                    worker.join(TIMEOUT)
                    assert not worker.is_alive()

            transfer()
            for identity in (portal, egress, vector):
                wait_for(lambda: get(identity)["tcps"] == 0)
                value = wait_for(lambda: get(identity) if get(identity)["tcprx"] == len(payload) else None)
                assert value["tcptx"] == len(payload)
                assert value["ping"] == value["pool"] == value["udps"] == 0
            for identity in (portal, egress, vector):
                assert not any("Flow " in text or "transfer starting" in text for text in logs(identity))
            with socket.create_connection(("127.0.0.1", ingress), TIMEOUT) as invalid:
                invalid.sendall(b"\x04\x01\x00")
                invalid.shutdown(socket.SHUT_WR)
                try:
                    invalid.recv(128)
                except ConnectionResetError:
                    pass
            wait_for(lambda: any("socks" in text.lower() for text in logs(vector)))
            action(vector, "stop")
            wait_for(lambda: get(vector)["status"] == "stopped")
            wait_for(lambda: any("shutdown complete" in text for text in logs(vector)))
            action(vector, "start")
            wait_for(lambda: len([text for text in logs(vector) if "starting:" in text]) == 2)
            transfer()
            wait_for(lambda: get(vector)["tcprx"] == 2 * len(payload))
            assert get(vector)["tcptx"] == 2 * len(payload)
            action(vector, "reset")
            time.sleep(1.2)
            assert get(vector)["tcprx"] == get(vector)["tcptx"] == 0
            invalid = create("vector://invalid@127.0.0.1:1")
            wait_for(lambda: get(invalid)["status"] == "error")
            wait_for(lambda: logs(invalid))
            assert not list((root / f"nowhere-telemetry-nw3-{os.getuid()}").glob("*invalid*"))
            action(portal, "stop")
            wait_for(lambda: get(portal)["status"] == "stopped")
            wait_for(lambda: any("Egress connection failed:" in text for text in logs(egress)))
            assert get(egress)["status"] == "running"
            for identity in (vector, egress):
                action(identity, "stop")
                wait_for(lambda: get(identity)["status"] == "stopped")
                wait_for(lambda: any("shutdown complete" in text for text in logs(identity)))
            action(vector, "start")
            wait_for(lambda: len([text for text in logs(vector) if "starting:" in text]) == 3)
            master.send_signal(signal.SIGTERM)
            master.wait(TIMEOUT)
            observer.join(TIMEOUT)
            assert not observer.is_alive()
            assert len([text for text in logs(vector) if "shutdown complete" in text]) == 3
            assert events[-1]["type"] == "shutdown"
            for identity in instances:
                for text in logs(identity):
                    assert KEY not in text and "invalid@" not in text
                    assert re.match(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z ", text)
                    assert "\x1b" not in text
            assert not errors, errors
            assert not list((root / f"nowhere-telemetry-nw3-{os.getuid()}").glob("*.json"))
            print(json.dumps({"result": "passed", "roles": ["portal", "egress", "vector"],
                "known_bytes_per_transfer": len(payload), "pipe_logs_over_sse": True,
                "active_flows": True, "cumulative_restart_and_reset": True,
                "egress_retry_keeps_running": True, "startup_failure_log": True,
                "master_shutdown_drains_child_logs": True}))
        finally:
            if master.poll() is None:
                master.send_signal(signal.SIGTERM)
            try:
                master.wait(TIMEOUT)
            except subprocess.TimeoutExpired:
                master.kill()
                master.wait()


if __name__ == "__main__":
    main()
