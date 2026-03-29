"""Tests for tools/cognirelay_client.py — issue #163."""

import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

# Import the client module from tools/ (not a package — use importlib)
import importlib.util

_client_path = os.path.join(os.path.dirname(__file__), "..", "tools", "cognirelay_client.py")
_spec = importlib.util.spec_from_file_location("cognirelay_client", _client_path)
client = importlib.util.module_from_spec(_spec)
# Register in sys.modules so that patch targets work
sys.modules["tools.cognirelay_client"] = client
sys.modules["cognirelay_client"] = client
_spec.loader.exec_module(client)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_namespace(**kwargs):
    """Build an argparse-like namespace with defaults for connection args."""
    defaults = {
        "token": None,
        "token_file": None,
        "token_env": None,
        "base_url": None,
        "timeout": 30.0,
    }
    defaults.update(kwargs)
    return type("NS", (), defaults)()


def _mock_urlopen(status=200, body=b"{}"):
    """Return a mock suitable for patching urllib.request.urlopen."""
    resp = MagicMock()
    resp.read.return_value = body
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def _run_client(*args, stdin_data=None, env_override=None):
    """Run cognirelay_client.py as a subprocess and return (returncode, stdout, stderr)."""
    env = os.environ.copy()
    # Remove vars that could interfere
    env.pop("COGNIRELAY_BASE_URL", None)
    env.pop("COGNIRELAY_TOKEN", None)
    if env_override:
        env.update(env_override)

    result = subprocess.run(
        [sys.executable, os.path.join(os.path.dirname(__file__), "..", "tools", "cognirelay_client.py"), *args],
        capture_output=True,
        text=True,
        input=stdin_data,
        env=env,
        timeout=10,
    )
    return result.returncode, result.stdout, result.stderr


# ===========================================================================
# Token resolution tests
# ===========================================================================

class TestTokenResolution(unittest.TestCase):
    """Token resolution precedence: --token > --token-file > --token-env > COGNIRELAY_TOKEN."""

    def test_explicit_token_wins(self):
        ns = _make_namespace(token="explicit", token_file="/dev/null", token_env="SOME_VAR")
        self.assertEqual(client.resolve_token(ns), "explicit")

    def test_token_file_wins_over_token_env(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("from-file\n")
            f.flush()
            path = f.name
        try:
            ns = _make_namespace(token=None, token_file=path, token_env="SOME_VAR")
            with patch.dict(os.environ, {"SOME_VAR": "from-env"}):
                self.assertEqual(client.resolve_token(ns), "from-file")
        finally:
            os.unlink(path)

    def test_token_env_wins_over_cognirelay_token(self):
        ns = _make_namespace(token=None, token_env="MY_TOK")
        with patch.dict(os.environ, {"MY_TOK": "from-named-env", "COGNIRELAY_TOKEN": "fallback"}):
            self.assertEqual(client.resolve_token(ns), "from-named-env")

    def test_cognirelay_token_implicit_fallback(self):
        ns = _make_namespace(token=None)
        with patch.dict(os.environ, {"COGNIRELAY_TOKEN": "fallback-tok"}):
            self.assertEqual(client.resolve_token(ns), "fallback-tok")

    def test_no_token_exits_3(self):
        ns = _make_namespace(token=None)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                client.resolve_token(ns)
            self.assertEqual(ctx.exception.code, 3)

    def test_token_file_missing_exits_3(self):
        ns = _make_namespace(token=None, token_file="/nonexistent/path/token.txt")
        with self.assertRaises(SystemExit) as ctx:
            client.resolve_token(ns)
        self.assertEqual(ctx.exception.code, 3)

    def test_token_env_unset_exits_3(self):
        ns = _make_namespace(token=None, token_env="UNSET_VAR_12345")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                client.resolve_token(ns)
            self.assertEqual(ctx.exception.code, 3)

    def test_token_file_empty_exits_3(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("")
            path = f.name
        try:
            ns = _make_namespace(token=None, token_file=path)
            with self.assertRaises(SystemExit) as ctx:
                client.resolve_token(ns)
            self.assertEqual(ctx.exception.code, 3)
        finally:
            os.unlink(path)

    def test_explicit_empty_token_exits_3(self):
        ns = _make_namespace(token="")
        with self.assertRaises(SystemExit) as ctx:
            client.resolve_token(ns)
        self.assertEqual(ctx.exception.code, 3)


# ===========================================================================
# Base URL tests
# ===========================================================================

class TestBaseURL(unittest.TestCase):
    """Base URL resolution and trailing-slash normalization."""

    def test_env_fallback(self):
        ns = _make_namespace(base_url=None)
        with patch.dict(os.environ, {"COGNIRELAY_BASE_URL": "http://localhost:8000"}):
            self.assertEqual(client.resolve_base_url(ns), "http://localhost:8000")

    def test_explicit_overrides_env(self):
        ns = _make_namespace(base_url="http://explicit:9000")
        with patch.dict(os.environ, {"COGNIRELAY_BASE_URL": "http://env:8000"}):
            self.assertEqual(client.resolve_base_url(ns), "http://explicit:9000")

    def test_trailing_slash_stripped(self):
        ns = _make_namespace(base_url="http://localhost:8000/")
        self.assertEqual(client.resolve_base_url(ns), "http://localhost:8000")

    def test_double_trailing_slash_only_one_stripped(self):
        ns = _make_namespace(base_url="http://localhost:8000//")
        self.assertEqual(client.resolve_base_url(ns), "http://localhost:8000/")

    def test_missing_base_url_exits_2(self):
        ns = _make_namespace(base_url=None)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                client.resolve_base_url(ns)
            self.assertEqual(ctx.exception.code, 2)


# ===========================================================================
# No subcommand test
# ===========================================================================

class TestNoSubcommand(unittest.TestCase):

    def test_no_subcommand_exits_2(self):
        rc, stdout, stderr = _run_client()
        self.assertEqual(rc, 2)
        self.assertIn("usage", stderr.lower())

    def test_token_no_subcommand_exits_2(self):
        rc, stdout, stderr = _run_client("token")
        self.assertEqual(rc, 2)
        self.assertIn("usage", stderr.lower())


# ===========================================================================
# read subcommand tests
# ===========================================================================

SAMPLE_READ_RESPONSE = {
    "source_state": "active",
    "recovery_warnings": [],
    "capsule": {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "agent-1",
        "continuity": {
            "top_priorities": ["Priority A", "Priority B"],
            "active_constraints": ["Constraint 1"],
            "open_loops": [],
            "negative_decisions": [
                {"decision": "No caching", "rationale": "Adds complexity"},
            ],
            "session_trajectory": ["Step 1", "Step 2"],
        },
    },
}

SAMPLE_NULL_CAPSULE_RESPONSE = {
    "source_state": "missing",
    "recovery_warnings": ["Capsule not found"],
    "capsule": None,
}


class TestReadSubcommand(unittest.TestCase):

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_json_unit(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_read(args)
            output = mock_out.getvalue()
        parsed = json.loads(output)
        self.assertEqual(parsed["source_state"], "active")
        self.assertIn("capsule", parsed)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_startup_legacy_fallback(self, mock_urlopen):
        """Response without startup_summary falls back to legacy format_startup()."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="startup",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_read(args)
            output = mock_out.getvalue()

        self.assertIn("=== Source State ===", output)
        self.assertIn("active", output)
        self.assertIn("=== Recovery Warnings ===", output)
        self.assertIn("=== Top Priorities ===", output)
        self.assertIn("- Priority A", output)
        self.assertIn("=== Active Constraints ===", output)
        self.assertIn("=== Open Loops ===", output)
        self.assertIn("=== Negative Decisions ===", output)
        self.assertIn("- No caching: Adds complexity", output)
        self.assertIn("=== Session Trajectory ===", output)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_startup_null_capsule(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_NULL_CAPSULE_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="startup",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_read(args)
            output = mock_out.getvalue()

        self.assertIn("=== Source State ===", output)
        self.assertIn("missing", output)
        self.assertIn("- Capsule not found", output)
        self.assertIn("(no capsule available)", output)
        # Capsule sections must not appear
        self.assertNotIn("=== Top Priorities ===", output)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_startup_missing_optional_fields(self, mock_urlopen):
        """Missing/null capsule fields treated as empty lists in legacy fallback."""
        response = {
            "source_state": "active",
            "recovery_warnings": [],
            "capsule": {
                "continuity": {
                    "top_priorities": None,
                    "open_loops": [],
                    "negative_decisions": None,
                },
            },
        }
        mock_urlopen.return_value = _mock_urlopen(body=json.dumps(response).encode())
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="startup",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_read(args)
            output = mock_out.getvalue()

        lines = output.split("\n")
        for header in ["Top Priorities", "Active Constraints", "Open Loops",
                        "Negative Decisions", "Session Trajectory"]:
            idx = next(i for i, line in enumerate(lines) if header in line)
            self.assertEqual(lines[idx + 1], "(none)")

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_output_to_file(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            outpath = f.name
        try:
            args = _make_namespace(
                token="test-token",
                base_url="http://localhost:8000",
                command="read",
                subject_kind="user",
                subject_id="agent-1",
                format="json",
                output=outpath,
            )
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                client.cmd_read(args)
                self.assertEqual(mock_out.getvalue(), "")
            with open(outpath) as f:
                content = f.read()
            parsed = json.loads(content)
            self.assertEqual(parsed["source_state"], "active")
        finally:
            os.unlink(outpath)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_http_401_exits_1(self, mock_urlopen):
        exc = urllib.error.HTTPError(
            "http://x", 401, "Unauthorized", {}, io.BytesIO(b"unauthorized")
        )
        mock_urlopen.side_effect = exc
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_read(args)
        self.assertEqual(ctx.exception.code, 1)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_http_500_exits_1(self, mock_urlopen):
        exc = urllib.error.HTTPError(
            "http://x", 500, "Server Error", {}, io.BytesIO(b"internal error")
        )
        mock_urlopen.side_effect = exc
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_read(args)
        self.assertEqual(ctx.exception.code, 1)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_connection_refused_exits_4(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_read(args)
        self.assertEqual(ctx.exception.code, 4)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_timeout_exits_4(self, mock_urlopen):
        mock_urlopen.side_effect = TimeoutError("timed out")
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_read(args)
        self.assertEqual(ctx.exception.code, 4)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_non_json_response_exits_5(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(body=b"<html>not json</html>")
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_read(args)
        self.assertEqual(ctx.exception.code, 5)

    def test_read_missing_subject_kind_exits_2(self):
        rc, stdout, stderr = _run_client(
            "read", "--subject-id", "x",
            env_override={"COGNIRELAY_BASE_URL": "http://localhost:8000", "COGNIRELAY_TOKEN": "tok"},
        )
        self.assertEqual(rc, 2)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_startup_sends_view_startup(self, mock_urlopen):
        """--format startup includes view=startup in request body."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="startup",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO):
            client.cmd_read(args)
        request_obj = mock_urlopen.call_args[0][0]
        body = json.loads(request_obj.data.decode())
        self.assertEqual(body["view"], "startup")

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_json_does_not_send_view(self, mock_urlopen):
        """--format json does not include view in request body."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO):
            client.cmd_read(args)
        request_obj = mock_urlopen.call_args[0][0]
        body = json.loads(request_obj.data.decode())
        self.assertNotIn("view", body)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_allow_fallback_always_true(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO):
            client.cmd_read(args)
        # Check the request body
        call_args = mock_urlopen.call_args
        request_obj = call_args[0][0]
        body = json.loads(request_obj.data.decode())
        self.assertTrue(body["allow_fallback"])

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_startup_malformed_negative_decisions(self, mock_urlopen):
        """Malformed negative_decisions items degrade gracefully, no crash."""
        response = {
            "source_state": "active",
            "recovery_warnings": [],
            "capsule": {
                "continuity": {
                    "top_priorities": [],
                    "active_constraints": [],
                    "open_loops": [],
                    "negative_decisions": [
                        {"decision": "Good"},  # missing rationale
                        {"rationale": "reason"},  # missing decision
                        "bare-string",  # not a dict
                    ],
                    "session_trajectory": [],
                },
            },
        }
        mock_urlopen.return_value = _mock_urlopen(body=json.dumps(response).encode())
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="startup",
            output=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_read(args)
            output = mock_out.getvalue()
        self.assertIn("- Good: ", output)
        self.assertIn("- : reason", output)
        self.assertIn("- bare-string: ", output)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_read_output_unwritable_exits_2(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_READ_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="read",
            subject_kind="user",
            subject_id="agent-1",
            format="json",
            output="/nonexistent/dir/output.json",
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_read(args)
        self.assertEqual(ctx.exception.code, 2)


# ===========================================================================
# upsert subcommand tests
# ===========================================================================

SAMPLE_UPSERT_PAYLOAD = {
    "subject_kind": "user",
    "subject_id": "agent-1",
    "capsule": {"top_priorities": ["Test"]},
}

SAMPLE_UPSERT_RESPONSE = {"status": "ok", "version": 2}


class TestUpsertSubcommand(unittest.TestCase):

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_upsert_from_input_file(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_UPSERT_RESPONSE).encode()
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(SAMPLE_UPSERT_PAYLOAD, f)
            inpath = f.name
        try:
            args = _make_namespace(
                token="test-token",
                base_url="http://localhost:8000",
                command="upsert",
                input=inpath,
                stdin=False,
            )
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                client.cmd_upsert(args)
                output = mock_out.getvalue()
            parsed = json.loads(output)
            self.assertEqual(parsed["status"], "ok")
        finally:
            os.unlink(inpath)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_upsert_from_stdin(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_UPSERT_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="upsert",
            input=None,
            stdin=True,
        )
        stdin_data = json.dumps(SAMPLE_UPSERT_PAYLOAD).encode()
        with patch("sys.stdin", new_callable=lambda: MagicMock()) as mock_stdin:
            mock_stdin.buffer = io.BytesIO(stdin_data)
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                client.cmd_upsert(args)
                output = mock_out.getvalue()
        parsed = json.loads(output)
        self.assertEqual(parsed["status"], "ok")

    def test_upsert_both_input_and_stdin_exits_2(self):
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="upsert",
            input="/some/file",
            stdin=True,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_upsert(args)
        self.assertEqual(ctx.exception.code, 2)

    def test_upsert_neither_input_nor_stdin_exits_2(self):
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="upsert",
            input=None,
            stdin=False,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_upsert(args)
        self.assertEqual(ctx.exception.code, 2)

    def test_upsert_input_file_not_found_exits_2(self):
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="upsert",
            input="/nonexistent/file.json",
            stdin=False,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_upsert(args)
        self.assertEqual(ctx.exception.code, 2)

    def test_upsert_payload_over_256k_exits_2(self):
        big_data = b"x" * (client.MAX_PAYLOAD_BYTES + 1)
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as f:
            f.write(big_data)
            inpath = f.name
        try:
            args = _make_namespace(
                token="test-token",
                base_url="http://localhost:8000",
                command="upsert",
                input=inpath,
                stdin=False,
            )
            with self.assertRaises(SystemExit) as ctx:
                client.cmd_upsert(args)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            os.unlink(inpath)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_upsert_server_400_exits_1(self, mock_urlopen):
        exc = urllib.error.HTTPError(
            "http://x", 400, "Bad Request", {}, io.BytesIO(b"bad payload")
        )
        mock_urlopen.side_effect = exc
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"bad": "payload"}, f)
            inpath = f.name
        try:
            args = _make_namespace(
                token="test-token",
                base_url="http://localhost:8000",
                command="upsert",
                input=inpath,
                stdin=False,
            )
            with self.assertRaises(SystemExit) as ctx:
                client.cmd_upsert(args)
            self.assertEqual(ctx.exception.code, 1)
        finally:
            os.unlink(inpath)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_upsert_payload_sent_verbatim(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_UPSERT_RESPONSE).encode()
        )
        raw_payload = json.dumps(SAMPLE_UPSERT_PAYLOAD, separators=(",", ":"))
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(raw_payload)
            inpath = f.name
        try:
            args = _make_namespace(
                token="test-token",
                base_url="http://localhost:8000",
                command="upsert",
                input=inpath,
                stdin=False,
            )
            with patch("sys.stdout", new_callable=io.StringIO):
                client.cmd_upsert(args)
            call_args = mock_urlopen.call_args
            request_obj = call_args[0][0]
            self.assertEqual(request_obj.data, raw_payload.encode())
        finally:
            os.unlink(inpath)


# ===========================================================================
# token hash subcommand tests
# ===========================================================================

class TestTokenHash(unittest.TestCase):

    KNOWN_INPUT = "my-secret-token"
    KNOWN_DIGEST = hashlib.sha256(b"my-secret-token").hexdigest()

    def test_hash_value(self):
        args = _make_namespace(value=self.KNOWN_INPUT, file=None, env=None)
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_token_hash(args)
            output = mock_out.getvalue()
        self.assertEqual(output.strip(), self.KNOWN_DIGEST)

    def test_hash_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(self.KNOWN_INPUT + "\n")
            path = f.name
        try:
            args = _make_namespace(value=None, file=path, env=None)
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                client.cmd_token_hash(args)
                output = mock_out.getvalue()
            self.assertEqual(output.strip(), self.KNOWN_DIGEST)
        finally:
            os.unlink(path)

    def test_hash_env(self):
        args = _make_namespace(value=None, file=None, env="TEST_TOKEN_VAR")
        with patch.dict(os.environ, {"TEST_TOKEN_VAR": self.KNOWN_INPUT + "  \n"}):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
                client.cmd_token_hash(args)
                output = mock_out.getvalue()
        self.assertEqual(output.strip(), self.KNOWN_DIGEST)

    def test_hash_multiple_sources_exits_2(self):
        args = _make_namespace(value="tok", file="/some/file", env=None)
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_token_hash(args)
        self.assertEqual(ctx.exception.code, 2)

    def test_hash_no_source_exits_2(self):
        args = _make_namespace(value=None, file=None, env=None)
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_token_hash(args)
        self.assertEqual(ctx.exception.code, 2)

    def test_hash_empty_value_exits_6(self):
        args = _make_namespace(value="", file=None, env=None)
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_token_hash(args)
        self.assertEqual(ctx.exception.code, 6)

    def test_hash_whitespace_only_file_exits_6(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("   \n")
            path = f.name
        try:
            args = _make_namespace(value=None, file=path, env=None)
            with self.assertRaises(SystemExit) as ctx:
                client.cmd_token_hash(args)
            self.assertEqual(ctx.exception.code, 6)
        finally:
            os.unlink(path)

    def test_hash_missing_file_exits_6(self):
        args = _make_namespace(value=None, file="/nonexistent/token.txt", env=None)
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_token_hash(args)
        self.assertEqual(ctx.exception.code, 6)

    def test_hash_unset_env_exits_6(self):
        args = _make_namespace(value=None, file=None, env="NONEXISTENT_VAR_XYZ")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                client.cmd_token_hash(args)
            self.assertEqual(ctx.exception.code, 6)

    def test_hash_whitespace_only_env_exits_6(self):
        args = _make_namespace(value=None, file=None, env="WS_ONLY_VAR")
        with patch.dict(os.environ, {"WS_ONLY_VAR": "   \n"}):
            with self.assertRaises(SystemExit) as ctx:
                client.cmd_token_hash(args)
            self.assertEqual(ctx.exception.code, 6)

    def test_hash_output_bare_hex_plus_newline(self):
        args = _make_namespace(value="test", file=None, env=None)
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_token_hash(args)
            output = mock_out.getvalue()
        # Must be exactly hex + \n
        self.assertTrue(output.endswith("\n"))
        hex_part = output.rstrip("\n")
        self.assertRegex(hex_part, r"^[0-9a-f]{64}$")
        self.assertEqual(len(output), 65)  # 64 hex chars + 1 newline

    def test_hash_known_answer(self):
        """Digest must match known SHA-256 for a given input."""
        token = "compatibility-test-token"
        expected = hashlib.sha256(token.encode("utf-8")).hexdigest()
        args = _make_namespace(value=token, file=None, env=None)
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_token_hash(args)
            output = mock_out.getvalue()
        self.assertEqual(output.strip(), expected)


# ===========================================================================
# format_startup_summary tests (server-side startup_summary rendering)
# ===========================================================================

# Full startup_summary response fixture
SAMPLE_STARTUP_SUMMARY_RESPONSE = {
    "source_state": "active",
    "capsule": {
        "thread_descriptor": {
            "label": "Auth refactor",
            "lifecycle": "active",
            "keywords": ["auth", "security"],
        },
    },
    "startup_summary": {
        "recovery": {
            "source_state": "active",
            "recovery_warnings": ["Stale capsule detected"],
            "capsule_health_status": "degraded",
            "capsule_health_reasons": ["Missing verification"],
        },
        "orientation": {
            "top_priorities": ["Ship v2 API", "Fix auth bug"],
            "active_constraints": ["No breaking changes"],
            "open_loops": ["Review PR #42"],
            "negative_decisions": [
                {"decision": "No caching", "rationale": "Adds complexity"},
            ],
            "rationale_entries": [
                {"kind": "constraint", "tag": "perf", "summary": "Latency budget is 200ms"},
            ],
        },
        "context": {
            "session_trajectory": ["Started auth extraction", "Reviewed PR"],
            "stance_summary": "Wrapping up auth work.",
            "active_concerns": ["Deployment risk"],
        },
        "updated_at": "2026-03-29T10:00:00Z",
        "trust_signals": {
            "recency": {"phase": "current", "freshness_class": "fresh"},
            "completeness": {"orientation_adequate": True, "trimmed": False},
            "integrity": {"health_status": "healthy", "verification_status": "verified"},
            "scope_match": {"exact": True},
        },
        "stable_preferences": [
            {"tag": "communication", "content": "Prefer concise responses"},
        ],
    },
}


class TestFormatStartupSummary(unittest.TestCase):
    """Tests for format_startup_summary() rendering."""

    def test_format_startup_summary_full(self):
        """Full startup_summary + capsule with thread_descriptor renders all sections."""
        output = client.format_startup_summary(SAMPLE_STARTUP_SUMMARY_RESPONSE)

        # Always-shown sections
        self.assertIn("=== Source State ===", output)
        self.assertIn("active", output)
        self.assertIn("=== Top Priorities ===", output)
        self.assertIn("- Ship v2 API", output)
        self.assertIn("=== Active Constraints ===", output)
        self.assertIn("- No breaking changes", output)
        self.assertIn("=== Open Loops ===", output)
        self.assertIn("- Review PR #42", output)

        # Conditional sections — all present in this fixture
        self.assertIn("=== Recovery Warnings ===", output)
        self.assertIn("Health: degraded", output)
        self.assertIn("- Stale capsule detected", output)
        self.assertIn("- Missing verification", output)
        self.assertIn("=== Trust Signals ===", output)
        self.assertIn("=== Thread Identity ===", output)
        self.assertIn("Auth refactor [active]", output)
        self.assertIn("Keywords: auth, security", output)
        self.assertIn("=== Negative Decisions ===", output)
        self.assertIn("- No caching: Adds complexity", output)
        self.assertIn("=== Rationale Entries ===", output)
        self.assertIn("- [constraint] perf: Latency budget is 200ms", output)
        self.assertIn("=== Session Trajectory ===", output)
        self.assertIn("- Started auth extraction", output)
        self.assertIn("=== Stable Preferences ===", output)
        self.assertIn("- [communication] Prefer concise responses", output)

        # Not shown
        self.assertNotIn("updated_at", output)
        self.assertNotIn("stance_summary", output)
        self.assertNotIn("active_concerns", output)

    def test_format_startup_summary_always_shown_empty(self):
        """Null orientation renders always-shown sections with (none), suppresses conditional."""
        data = {
            "source_state": "active",
            "capsule": {},
            "startup_summary": {
                "recovery": {"source_state": "active"},
                "orientation": None,
                "context": None,
                "trust_signals": None,
                "stable_preferences": None,
            },
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Source State ===", output)
        self.assertIn("=== Top Priorities ===", output)
        self.assertIn("=== Active Constraints ===", output)
        self.assertIn("=== Open Loops ===", output)

        # Always-shown sections show (none) when empty
        lines = output.split("\n")
        for header in ["Top Priorities", "Active Constraints", "Open Loops"]:
            idx = next(i for i, line in enumerate(lines) if header in line)
            self.assertEqual(lines[idx + 1], "(none)")

        # Conditional sections suppressed
        self.assertNotIn("=== Recovery Warnings ===", output)
        self.assertNotIn("=== Trust Signals ===", output)
        self.assertNotIn("=== Thread Identity ===", output)
        self.assertNotIn("=== Negative Decisions ===", output)
        self.assertNotIn("=== Rationale Entries ===", output)
        self.assertNotIn("=== Session Trajectory ===", output)
        self.assertNotIn("=== Stable Preferences ===", output)

    def test_format_startup_summary_conditional_suppressed(self):
        """Empty conditional fields produce no output for those sections."""
        data = {
            "source_state": "active",
            "capsule": {},  # No thread_descriptor
            "startup_summary": {
                "recovery": {
                    "source_state": "active",
                    "recovery_warnings": [],
                    "capsule_health_reasons": [],
                },
                "orientation": {
                    "top_priorities": ["P1"],
                    "active_constraints": [],
                    "open_loops": [],
                    "negative_decisions": [],
                    "rationale_entries": [],
                },
                "context": {"session_trajectory": []},
                "trust_signals": None,
                "stable_preferences": [],
            },
        }
        output = client.format_startup_summary(data)

        self.assertNotIn("=== Recovery Warnings ===", output)
        self.assertNotIn("=== Trust Signals ===", output)
        self.assertNotIn("=== Thread Identity ===", output)
        self.assertNotIn("=== Negative Decisions ===", output)
        self.assertNotIn("=== Rationale Entries ===", output)
        self.assertNotIn("=== Session Trajectory ===", output)
        self.assertNotIn("=== Stable Preferences ===", output)

    def test_format_startup_summary_fallback(self):
        """Response lacking startup_summary falls back to legacy format_startup()."""
        data = {
            "source_state": "active",
            "recovery_warnings": [],
            "capsule": {
                "continuity": {
                    "top_priorities": ["Fallback priority"],
                    "active_constraints": [],
                    "open_loops": [],
                    "negative_decisions": [],
                    "session_trajectory": [],
                },
            },
        }
        output = client.format_startup_summary(data)

        # Legacy renderer output
        self.assertIn("=== Source State ===", output)
        self.assertIn("=== Recovery Warnings ===", output)
        self.assertIn("- Fallback priority", output)

    def test_format_startup_summary_no_capsule(self):
        """Null startup_summary + null capsule renders source state and placeholder."""
        data = {
            "source_state": "missing",
            "recovery_warnings": ["Capsule not found"],
            "startup_summary": None,
            "capsule": None,
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Source State ===", output)
        self.assertIn("missing", output)
        self.assertIn("=== Recovery Warnings ===", output)
        self.assertIn("- Capsule not found", output)
        self.assertIn("(no capsule available)", output)
        self.assertNotIn("=== Top Priorities ===", output)

    def test_format_startup_summary_no_capsule_no_warnings(self):
        """Null capsule with no warnings suppresses Recovery Warnings section."""
        data = {
            "source_state": "missing",
            "startup_summary": None,
            "capsule": None,
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Source State ===", output)
        self.assertIn("(no capsule available)", output)
        self.assertNotIn("=== Recovery Warnings ===", output)

    def test_format_startup_summary_trust_signals(self):
        """Trust signals digest renders 4 lines with correct field mapping."""
        data = {
            "source_state": "active",
            "capsule": {},
            "startup_summary": {
                "recovery": {"source_state": "active"},
                "orientation": {
                    "top_priorities": ["P1"],
                    "active_constraints": [],
                    "open_loops": [],
                },
                "trust_signals": {
                    "recency": {"phase": "current", "freshness_class": "fresh"},
                    "completeness": {"orientation_adequate": True, "trimmed": True},
                    "integrity": {"health_status": "healthy", "verification_status": "verified"},
                    "scope_match": {"exact": False},
                },
            },
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Trust Signals ===", output)
        self.assertIn("Recency: current (fresh)", output)
        self.assertIn("Completeness: orientation adequate, trimmed", output)
        self.assertIn("Integrity: healthy, verified", output)
        self.assertIn("Scope: fallback", output)

    def test_format_startup_summary_thread_identity(self):
        """Thread descriptor renders label, lifecycle, keywords."""
        data = {
            "source_state": "active",
            "capsule": {
                "thread_descriptor": {
                    "label": "Migration task",
                    "lifecycle": "suspended",
                    "keywords": ["db", "migration"],
                },
            },
            "startup_summary": {
                "recovery": {"source_state": "active"},
                "orientation": {
                    "top_priorities": [],
                    "active_constraints": [],
                    "open_loops": [],
                },
            },
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Thread Identity ===", output)
        self.assertIn("Migration task [suspended]", output)
        self.assertIn("Keywords: db, migration", output)

    def test_format_startup_summary_thread_identity_no_keywords(self):
        """Keywords line suppressed when keywords list is empty."""
        data = {
            "source_state": "active",
            "capsule": {
                "thread_descriptor": {
                    "label": "Simple thread",
                    "lifecycle": "active",
                    "keywords": [],
                },
            },
            "startup_summary": {
                "recovery": {"source_state": "active"},
                "orientation": {
                    "top_priorities": [],
                    "active_constraints": [],
                    "open_loops": [],
                },
            },
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Thread Identity ===", output)
        self.assertIn("Simple thread [active]", output)
        self.assertNotIn("Keywords:", output)

    def test_format_startup_summary_rationale_entries(self):
        """Rationale entries render as - [{kind}] {tag}: {summary}."""
        data = {
            "source_state": "active",
            "capsule": {},
            "startup_summary": {
                "recovery": {"source_state": "active"},
                "orientation": {
                    "top_priorities": ["P1"],
                    "active_constraints": [],
                    "open_loops": [],
                    "rationale_entries": [
                        {"kind": "decision", "tag": "arch", "summary": "Chose microservices"},
                        {"kind": "constraint", "tag": "perf", "summary": "Must be under 100ms"},
                    ],
                },
            },
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Rationale Entries ===", output)
        self.assertIn("- [decision] arch: Chose microservices", output)
        self.assertIn("- [constraint] perf: Must be under 100ms", output)

    def test_format_startup_summary_stable_preferences(self):
        """Stable preferences render as - [{tag}] {content}."""
        data = {
            "source_state": "active",
            "capsule": {},
            "startup_summary": {
                "recovery": {"source_state": "active"},
                "orientation": {
                    "top_priorities": [],
                    "active_constraints": [],
                    "open_loops": [],
                },
                "stable_preferences": [
                    {"tag": "tone", "content": "Be direct"},
                    {"tag": "format", "content": "Use bullet points"},
                ],
            },
        }
        output = client.format_startup_summary(data)

        self.assertIn("=== Stable Preferences ===", output)
        self.assertIn("- [tone] Be direct", output)
        self.assertIn("- [format] Use bullet points", output)


# ===========================================================================
# list subcommand tests
# ===========================================================================

SAMPLE_LIST_RESPONSE = {
    "ok": True,
    "count": 1,
    "capsules": [{"subject_kind": "user", "subject_id": "agent-1"}],
    "unique_match": False,
}


class TestListSubcommand(unittest.TestCase):

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_list_minimal(self, mock_urlopen):
        """list with no optional flags sends {} body and prints JSON."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_LIST_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="list",
            subject_kind=None,
            limit=None,
            include_fallback=False,
            include_archived=False,
            include_cold=False,
            lifecycle=None,
            scope_anchor=None,
            keyword=None,
            label_exact=None,
            anchor_kind=None,
            anchor_value=None,
            sort=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_list(args)
            output = mock_out.getvalue()

        # Verify request body is empty object
        request_obj = mock_urlopen.call_args[0][0]
        body = json.loads(request_obj.data.decode())
        self.assertEqual(body, {})

        # Verify output is pretty-printed JSON
        parsed = json.loads(output)
        self.assertTrue(parsed["ok"])

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_list_all_flags(self, mock_urlopen):
        """All flags map to correct request body fields."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_LIST_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="list",
            subject_kind="thread",
            limit=10,
            include_fallback=True,
            include_archived=True,
            include_cold=True,
            lifecycle="active",
            scope_anchor="project-x",
            keyword="auth",
            label_exact="Auth refactor",
            anchor_kind="project",
            anchor_value="cogni",
            sort="salience",
        )
        with patch("sys.stdout", new_callable=io.StringIO):
            client.cmd_list(args)

        request_obj = mock_urlopen.call_args[0][0]
        body = json.loads(request_obj.data.decode())
        self.assertEqual(body["subject_kind"], "thread")
        self.assertEqual(body["limit"], 10)
        self.assertTrue(body["include_fallback"])
        self.assertTrue(body["include_archived"])
        self.assertTrue(body["include_cold"])
        self.assertEqual(body["lifecycle"], "active")
        self.assertEqual(body["scope_anchor"], "project-x")
        self.assertEqual(body["keyword"], "auth")
        self.assertEqual(body["label_exact"], "Auth refactor")
        self.assertEqual(body["anchor_kind"], "project")
        self.assertEqual(body["anchor_value"], "cogni")
        self.assertEqual(body["sort"], "salience")

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_list_omits_unset_flags(self, mock_urlopen):
        """Omitted flags produce omitted fields (not null)."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_LIST_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="list",
            subject_kind="user",
            limit=None,
            include_fallback=False,
            include_archived=False,
            include_cold=False,
            lifecycle=None,
            scope_anchor=None,
            keyword=None,
            label_exact=None,
            anchor_kind=None,
            anchor_value=None,
            sort=None,
        )
        with patch("sys.stdout", new_callable=io.StringIO):
            client.cmd_list(args)

        request_obj = mock_urlopen.call_args[0][0]
        body = json.loads(request_obj.data.decode())
        # Only subject_kind should be present
        self.assertEqual(body, {"subject_kind": "user"})
        self.assertNotIn("limit", body)
        self.assertNotIn("include_fallback", body)
        self.assertNotIn("lifecycle", body)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_list_http_error_exits_1(self, mock_urlopen):
        exc = urllib.error.HTTPError(
            "http://x", 500, "Server Error", {}, io.BytesIO(b"error")
        )
        mock_urlopen.side_effect = exc
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="list",
            subject_kind=None,
            limit=None,
            include_fallback=False,
            include_archived=False,
            include_cold=False,
            lifecycle=None,
            scope_anchor=None,
            keyword=None,
            label_exact=None,
            anchor_kind=None,
            anchor_value=None,
            sort=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_list(args)
        self.assertEqual(ctx.exception.code, 1)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_list_connection_error_exits_4(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="list",
            subject_kind=None,
            limit=None,
            include_fallback=False,
            include_archived=False,
            include_cold=False,
            lifecycle=None,
            scope_anchor=None,
            keyword=None,
            label_exact=None,
            anchor_kind=None,
            anchor_value=None,
            sort=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_list(args)
        self.assertEqual(ctx.exception.code, 4)


# ===========================================================================
# capabilities subcommand tests
# ===========================================================================

SAMPLE_CAPABILITIES_RESPONSE = {
    "version": "1",
    "features": {
        "continuity.read.startup_view": {
            "summary": "Startup-oriented read view",
        },
    },
}


class TestCapabilitiesSubcommand(unittest.TestCase):

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_capabilities_success(self, mock_urlopen):
        """capabilities sends GET to /v1/capabilities and prints pretty JSON."""
        mock_urlopen.return_value = _mock_urlopen(
            body=json.dumps(SAMPLE_CAPABILITIES_RESPONSE).encode()
        )
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="capabilities",
        )
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            client.cmd_capabilities(args)
            output = mock_out.getvalue()

        # Verify GET method
        request_obj = mock_urlopen.call_args[0][0]
        self.assertEqual(request_obj.method, "GET")
        self.assertIsNone(request_obj.data)
        self.assertNotIn("Content-type", request_obj.headers)

        # Verify output
        parsed = json.loads(output)
        self.assertEqual(parsed["version"], "1")
        self.assertIn("features", parsed)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_capabilities_http_error_exits_1(self, mock_urlopen):
        exc = urllib.error.HTTPError(
            "http://x", 401, "Unauthorized", {}, io.BytesIO(b"unauthorized")
        )
        mock_urlopen.side_effect = exc
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="capabilities",
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_capabilities(args)
        self.assertEqual(ctx.exception.code, 1)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_capabilities_connection_error_exits_4(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        args = _make_namespace(
            token="test-token",
            base_url="http://localhost:8000",
            command="capabilities",
        )
        with self.assertRaises(SystemExit) as ctx:
            client.cmd_capabilities(args)
        self.assertEqual(ctx.exception.code, 4)


# ===========================================================================
# do_request GET/POST tests
# ===========================================================================

class TestDoRequest(unittest.TestCase):

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_do_request_get_no_body(self, mock_urlopen):
        """GET requests send no body and omit Content-Type."""
        mock_urlopen.return_value = _mock_urlopen(body=b'{"ok":true}')
        client.do_request(
            "http://localhost:8000", "/v1/capabilities", "tok", b"ignored", 30,
            method="GET",
        )
        request_obj = mock_urlopen.call_args[0][0]
        self.assertEqual(request_obj.method, "GET")
        self.assertIsNone(request_obj.data)
        self.assertNotIn("Content-type", request_obj.headers)
        self.assertIn("Authorization", request_obj.headers)

    @patch("tools.cognirelay_client.urllib.request.urlopen")
    def test_do_request_post_sends_body(self, mock_urlopen):
        """POST requests send body and include Content-Type."""
        mock_urlopen.return_value = _mock_urlopen(body=b'{"ok":true}')
        body = b'{"test": true}'
        client.do_request(
            "http://localhost:8000", "/v1/continuity/read", "tok", body, 30,
        )
        request_obj = mock_urlopen.call_args[0][0]
        self.assertEqual(request_obj.method, "POST")
        self.assertEqual(request_obj.data, body)
        self.assertEqual(request_obj.headers["Content-type"], "application/json")
        self.assertIn("Authorization", request_obj.headers)


if __name__ == "__main__":
    unittest.main()
