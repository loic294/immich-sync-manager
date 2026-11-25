import contextlib
import io
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

import health_summary


class LogSummaryTests(unittest.TestCase):
    def test_counts_errors_locks_and_queries_over_one_second(self):
        logs = io.StringIO(
            "2026-01-01T00:00:00Z INFO started\n"
            "2026-01-01T00:00:01Z ERROR worker: database is locked\n"
            "2026-01-01T00:00:02Z WARN sqlx: slow statement: elapsed_secs=1.000\n"
            "2026-01-01T00:00:03Z WARN sqlx: slow statement: elapsed_secs=1.5\n"
            "2026-01-01T00:00:04Z WARN sqlx: slow statement: elapsed_secs=unknown\n"
        )

        result = health_summary.summarize_logs(logs)

        self.assertEqual(result["records"], 5)
        self.assertEqual(result["errors"], 1)
        self.assertEqual(result["locked_busy"], 1)
        self.assertEqual(result["slow_over_one_second"], 1)
        self.assertEqual(result["unclassified_slow"], 1)
        self.assertEqual(result["first"], "2026-01-01T00:00:00Z")
        self.assertEqual(result["last"], "2026-01-01T00:00:04Z")

    def test_empty_logs_are_not_a_clean_window(self):
        result = health_summary.summarize_logs(io.StringIO(""))
        self.assertEqual(result["records"], 0)
        self.assertIsNone(result["first"])
        self.assertIsNone(result["last"])


class AttentionSummaryTests(unittest.TestCase):
    def test_reports_groups_without_summing_overlapping_items(self):
        result = health_summary.summarize_attention(
            {
                "observed_at": "2026-01-01T00:00:00Z",
                "completeness": "partial",
                "items": [
                    {
                        "id": "transfer:blocked",
                        "state": "blocked", "blocks_work": True, "count": 10,
                        "title": "Blocked transfers", "message": "The peer is offline.",
                        "next_at": None,
                        "examples": [{"resource": "photo.example.jpg", "detail": None}],
                    },
                    {
                        "id": "transfer:waiting",
                        "state": "waiting", "blocks_work": True, "count": 10,
                        "title": "Waiting transfers", "message": "Outside allowed hours.",
                        "next_at": "2026-01-01T01:00:00Z",
                        "examples": [],
                    },
                    {
                        "id": "operation:failed",
                        "state": "failed", "blocks_work": False, "count": 2,
                        "title": "Failed transfers", "message": "Recovery is needed.",
                        "next_at": None,
                        "examples": [],
                    },
                ],
            }
        )

        self.assertEqual(result["completeness"], "partial")
        self.assertEqual(result["by_state"]["blocked"], 1)
        self.assertEqual(result["by_state"]["waiting"], 1)
        self.assertEqual(result["by_state"]["failed"], 1)
        self.assertEqual(result["blocking_groups"], 2)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            health_summary.print_attention(result)
        self.assertIn("Blocked transfers", output.getvalue())
        self.assertIn("The peer is offline.", output.getvalue())
        self.assertIn("2026-01-01T01:00:00Z", output.getvalue())
        self.assertIn("transfer:blocked", output.getvalue())
        self.assertIn("photo.example.jpg", output.getvalue())

    def test_rejects_missing_or_invalid_evidence(self):
        with self.assertRaises(ValueError):
            health_summary.summarize_attention({"items": []})
        with self.assertRaises(ValueError):
            health_summary.summarize_attention(
                {
                    "observed_at": "2026-01-01T00:00:00Z",
                    "completeness": "complete",
                    "items": [{"state": "blocked", "blocks_work": "true"}],
                }
            )
        with self.assertRaises(ValueError):
            health_summary.summarize_attention(
                {
                    "observed_at": "2026-01-01T00:00:00Z",
                    "completeness": {},
                    "items": [],
                }
            )
        with self.assertRaises(ValueError):
            health_summary.summarize_attention(
                {
                    "observed_at": "2026-01-01T00:00:00Z",
                    "completeness": "complete",
                    "items": [{
                        "id": None, "state": "failed", "blocks_work": False, "count": 1,
                        "title": "Failed", "message": "Review", "next_at": None, "examples": [],
                    }],
                }
            )

    def test_partial_or_missing_evidence_is_not_reported_as_success(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "runtime.log"
            snapshot = Path(directory) / "attention.json"
            log.write_text("2026-01-01T00:00:00Z ERROR database is busy\n")
            snapshot.write_text(
                json.dumps(
                    {
                        "observed_at": "2026-01-01T00:00:00Z",
                        "completeness": "partial",
                        "items": [{
                            "id": "transfer:blocked",
                            "state": "blocked", "blocks_work": True, "count": 1,
                            "title": "Blocked", "message": "The peer is offline.",
                            "next_at": None,
                            "examples": [],
                        }],
                    }
                )
            )
            output = io.StringIO()
            with mock.patch.object(health_summary, "fetch_health"), contextlib.redirect_stdout(
                output
            ):
                status = health_summary.main(
                    [
                        "--instance",
                        "example",
                        "https://east.example",
                        str(log),
                        "--snapshot",
                        "example",
                        str(snapshot),
                    ]
                )
            self.assertEqual(status, 1)
            self.assertIn("attention: partial", output.getvalue())
            self.assertIn("locked/busy=1", output.getvalue())


class AuthenticationTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.logout_status = 204
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def respond(self, status, body=b"", cookie=False):
                self.send_response(status)
                if cookie:
                    self.send_header("Set-Cookie", "ism_session=synthetic; Path=/")
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                test.calls.append(self.path)
                if self.path == "/healthz":
                    self.respond(200, b"ok")
                elif self.path == "/api/admin/attention" and self.headers.get("Cookie") == "ism_session=synthetic":
                    self.respond(200, json.dumps({
                        "observed_at": "2026-01-01T00:00:00Z",
                        "completeness": "complete",
                        "items": [{
                            "id": "transfer:stalled",
                            "state": "stalled", "blocks_work": True, "count": 1,
                            "title": "A transfer stalled", "message": "The peer is offline.",
                            "next_at": None,
                            "examples": [],
                        }],
                    }).encode())
                else:
                    self.respond(401)

            def do_POST(self):
                test.calls.append(self.path)
                if self.headers.get("x-ism-request") != "1":
                    self.respond(403)
                elif self.path == "/api/admin/login":
                    length = int(self.headers["Content-Length"])
                    credentials = json.loads(self.rfile.read(length))
                    if credentials == {"username": "synthetic", "password": "synthetic-password"}:
                        self.respond(200, cookie=True)
                    else:
                        self.respond(401)
                elif self.path == "/api/admin/logout" and self.headers.get("Cookie") == "ism_session=synthetic":
                    self.respond(test.logout_status)
                else:
                    self.respond(401)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.thread.join)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def run_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "runtime.log"
            log.write_text("2026-01-01T00:00:00Z INFO started\n")
            output = io.StringIO()
            with mock.patch.dict(
                os.environ, {"ISM_TEST_USER": "synthetic", "ISM_TEST_PASSWORD": "synthetic-password"}
            ), contextlib.redirect_stdout(output):
                status = health_summary.main([
                    "--instance", "example", self.url, str(log),
                    "--credentials", "example", "ISM_TEST_USER", "ISM_TEST_PASSWORD",
                ])
            return status, output.getvalue()

    def test_summary_uses_session_and_shows_stuck_reasons(self):
        status, output = self.run_summary()
        self.assertEqual(status, 1)
        self.assertIn("A transfer stalled", output)
        self.assertIn("The peer is offline.", output)
        self.assertEqual(
            self.calls, ["/healthz", "/api/admin/login", "/api/admin/attention", "/api/admin/logout"]
        )

    def test_logout_failure_is_reported(self):
        self.logout_status = 503
        status, output = self.run_summary()
        self.assertEqual(status, 1)
        self.assertIn("attention: unavailable (logout failed: HTTP 503)", output)
        self.assertIn("/api/admin/logout", self.calls)


if __name__ == "__main__":
    unittest.main()
