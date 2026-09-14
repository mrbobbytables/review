#!/usr/bin/env python3
"""Contract for client-side broker wire protocol and TUI clients (#492).

Verifies that:
- `read_response_line` bounds allocations at MAX_RESPONSE_BYTES and handles chunk splits.
- `client_request` frames version and session, connects over unix sockets, and parses responses.
- `review_exec_client` and `lab_client` both delegate framing and byte caps to `broker_protocol`.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(REPO_ROOT / "image"))

from tui import broker_protocol as protocol
from tui import lab_client, review_exec_client


class FakeConnection:
    """Feeds `read_response_line` a scripted sequence of recv() results."""

    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.recv_calls = 0

    def recv(self, _size):
        self.recv_calls += 1
        if not self.chunks:
            return b""
        return self.chunks.pop(0)


class ReadResponseLineTests(unittest.TestCase):
    def test_response_ends_at_the_first_newline(self):
        connection = FakeConnection([b'{"version":1,"ok":true}\n{"extra":true}\n'])
        self.assertEqual(
            protocol.read_response_line(connection),
            b'{"version":1,"ok":true}',
        )

    def test_response_split_across_recv_calls_is_reassembled(self):
        connection = FakeConnection([b'{"version":', b'1,"ok":', b"true}\n"])
        self.assertEqual(
            protocol.read_response_line(connection),
            b'{"version":1,"ok":true}',
        )

    def test_response_without_newline_at_eof_is_accepted(self):
        connection = FakeConnection([b'{"version":1,"ok":true}'])
        self.assertEqual(
            protocol.read_response_line(connection),
            b'{"version":1,"ok":true}',
        )

    def test_empty_connection_raises_eof(self):
        with self.assertRaises(EOFError):
            protocol.read_response_line(FakeConnection([]))

    def test_oversized_response_raises_value_error(self):
        oversized = [b"x" * 1024] * 300  # 307200 > 262144
        with self.assertRaises(ValueError) as failure:
            protocol.read_response_line(FakeConnection(oversized), max_bytes=protocol.MAX_RESPONSE_BYTES)
        self.assertIn("exceeded the response bound", str(failure.exception))


class ClientRequestTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.temp_dir.name, "test.sock")

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_one_shot_server(self, responder):
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.socket_path)
        server.listen(1)

        def serve():
            try:
                conn, _ = server.accept()
                with conn:
                    responder(conn)
            finally:
                server.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        return thread

    def test_client_request_empty_path_raises_value_error(self):
        with self.assertRaises(ValueError):
            protocol.client_request("", {"action": "status"}, session="sess")

    def test_client_request_connection_refused_raises_os_error(self):
        with self.assertRaises(OSError):
            protocol.client_request(self.socket_path, {"action": "status"}, session="sess", timeout=1.0)

    def test_client_request_success(self):
        def responder(conn):
            raw = protocol.read_request_line(conn)
            req = json.loads(raw.decode("utf-8"))
            self.assertEqual(req["version"], protocol.PROTOCOL_VERSION)
            self.assertEqual(req["session"], "test-session")
            self.assertEqual(req["action"], "status")
            conn.sendall(protocol.json_line({"version": 1, "ok": True, "state": "READY"}))

        thread = self.run_one_shot_server(responder)
        answer = protocol.client_request(
            self.socket_path,
            {"action": "status"},
            session="test-session",
            timeout=5.0,
        )
        thread.join(timeout=2.0)
        self.assertEqual(answer, {"version": 1, "ok": True, "state": "READY"})

    def test_client_request_unparseable_response(self):
        def responder(conn):
            conn.recv(1024)
            conn.sendall(b"not json\n")

        thread = self.run_one_shot_server(responder)
        with self.assertRaises(ValueError) as failure:
            protocol.client_request(
                self.socket_path,
                {"action": "status"},
                session="test-session",
                timeout=5.0,
            )
        thread.join(timeout=2.0)
        self.assertIn("unparseable broker answer", str(failure.exception))

    def test_client_request_non_object_response(self):
        def responder(conn):
            conn.recv(1024)
            conn.sendall(b'"string"\n')

        thread = self.run_one_shot_server(responder)
        with self.assertRaises(ValueError) as failure:
            protocol.client_request(
                self.socket_path,
                {"action": "status"},
                session="test-session",
                timeout=5.0,
            )
        thread.join(timeout=2.0)
        self.assertIn("broker answer was not an object", str(failure.exception))

    def test_client_request_unsupported_version(self):
        def responder(conn):
            conn.recv(1024)
            conn.sendall(b'{"version":99,"ok":true}\n')

        thread = self.run_one_shot_server(responder)
        with self.assertRaises(ValueError) as failure:
            protocol.client_request(
                self.socket_path,
                {"action": "status"},
                session="test-session",
                timeout=5.0,
            )
        thread.join(timeout=2.0)
        self.assertIn("unsupported broker protocol", str(failure.exception))

    def test_client_request_oversized_response(self):
        def responder(conn):
            conn.recv(1024)
            flood = b"x" * (protocol.MAX_RESPONSE_BYTES + 100) + b"\n"
            conn.sendall(flood)

        thread = self.run_one_shot_server(responder)
        with self.assertRaises(ValueError) as failure:
            protocol.client_request(
                self.socket_path,
                {"action": "status"},
                session="test-session",
                timeout=5.0,
            )
        thread.join(timeout=2.0)
        self.assertIn("exceeded the response bound", str(failure.exception))


class ReviewExecClientTests(unittest.TestCase):
    def test_unconfigured_broker_raises_runtime_error(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as failure:
                review_exec_client.status()
            self.assertIn("not configured", str(failure.exception))

    def test_review_exec_client_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sock_path = os.path.join(temp_dir, "exec.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(sock_path)
            server.listen(1)

            def serve():
                conn, _ = server.accept()
                with conn:
                    raw = protocol.read_request_line(conn)
                    req = json.loads(raw.decode("utf-8"))
                    self.assertEqual(req["action"], "submit")
                    self.assertEqual(req["repository"], "org/repo")
                    conn.sendall(protocol.json_line({"version": 1, "ok": True, "result": "submitted", "job": "job-1"}))
                server.close()

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()

            with mock.patch.dict(
                os.environ,
                {
                    review_exec_client.SOCKET_ENV: sock_path,
                    review_exec_client.SESSION_ENV: "exec-sess",
                },
            ):
                res = review_exec_client.submit("org/repo", 10, "a" * 40, "b" * 40, "omp", "model", "high")
                self.assertEqual(res, {"version": 1, "ok": True, "result": "submitted", "job": "job-1"})
            thread.join(timeout=2.0)

    def test_review_exec_client_wraps_errors_in_runtime_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sock_path = os.path.join(temp_dir, "nonexistent.sock")
            with mock.patch.dict(
                os.environ,
                {
                    review_exec_client.SOCKET_ENV: sock_path,
                    review_exec_client.SESSION_ENV: "exec-sess",
                },
            ):
                with self.assertRaises(RuntimeError) as failure:
                    review_exec_client.status()
                self.assertIn("review-exec broker request failed", str(failure.exception))


class LabClientTests(unittest.TestCase):
    def test_unconfigured_lab_returns_off_envelope(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(lab_client.lab_configured())
            res = lab_client.status()
            self.assertEqual(res, {"ok": False, "state": "OFF", "error": "off", "detail": "no lab socket"})

    def test_lab_client_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sock_path = os.path.join(temp_dir, "lab.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(sock_path)
            server.listen(1)

            def serve():
                conn, _ = server.accept()
                with conn:
                    raw = protocol.read_request_line(conn)
                    req = json.loads(raw.decode("utf-8"))
                    self.assertEqual(req["action"], "status")
                    conn.sendall(protocol.json_line({"version": 1, "ok": True, "state": "READY"}))
                server.close()

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()

            with mock.patch.dict(
                os.environ,
                {
                    lab_client.SOCKET_ENV: sock_path,
                    lab_client.SESSION_ENV: "lab-sess",
                },
            ):
                self.assertTrue(lab_client.lab_configured())
                res = lab_client.status()
                self.assertEqual(res, {"version": 1, "ok": True, "state": "READY"})
            thread.join(timeout=2.0)

    def test_lab_client_connection_failure_degrades(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sock_path = os.path.join(temp_dir, "dead.sock")
            # Create a file that is not a socket
            Path(sock_path).write_text("")
            with mock.patch.dict(
                os.environ,
                {
                    lab_client.SOCKET_ENV: sock_path,
                    lab_client.SESSION_ENV: "lab-sess",
                },
            ):
                res = lab_client.status()
                self.assertFalse(res["ok"])
                self.assertEqual(res["state"], "DEGRADED")
                self.assertEqual(res["error"], "unreachable")


if __name__ == "__main__":
    unittest.main()
