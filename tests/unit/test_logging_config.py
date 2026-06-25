"""Unit tests for ``src.utils.logging_config``."""

from __future__ import annotations

import io
import json
import logging

import pytest

from src.utils.logging_config import JsonFormatter, setup_logging


@pytest.fixture(autouse=True)
def _reset_root_handlers():
    """setup_logging() mutates the root logger — restore it between tests."""
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    yield
    root.handlers = original_handlers
    root.setLevel(original_level)


def _make_record(**kwargs) -> logging.LogRecord:
    defaults = dict(
        name="test.logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    defaults.update(kwargs)
    return logging.LogRecord(**defaults)


class TestJsonFormatter:
    def test_emits_required_fields(self):
        formatter = JsonFormatter()
        record = _make_record()
        payload = json.loads(formatter.format(record))
        assert payload["level"] == "INFO"
        assert payload["logger"] == "test.logger"
        assert payload["message"] == "hello world"
        assert "timestamp" in payload

    def test_includes_extras(self):
        formatter = JsonFormatter()
        record = _make_record()
        record.match_id = "id-001"
        record.layer = "bronze"
        payload = json.loads(formatter.format(record))
        assert payload["match_id"] == "id-001"
        assert payload["layer"] == "bronze"

    def test_omits_reserved_logrecord_keys(self):
        formatter = JsonFormatter()
        record = _make_record()
        payload = json.loads(formatter.format(record))
        # Internal LogRecord plumbing should not leak into the JSON output
        for forbidden in ("msg", "args", "levelno", "pathname", "lineno"):
            assert forbidden not in payload

    def test_flattens_exception_traceback(self):
        formatter = JsonFormatter()
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            import sys

            record = _make_record(exc_info=sys.exc_info())
        payload = json.loads(formatter.format(record))
        assert "exception" in payload
        assert "RuntimeError: boom" in payload["exception"]


class TestSetupLogging:
    def test_defaults_to_text_format(self, monkeypatch, capsys):
        monkeypatch.delenv("LOG_FORMAT", raising=False)
        setup_logging("INFO")
        logging.getLogger("test").info("plain text")
        out = capsys.readouterr().out
        assert "plain text" in out
        assert "[INFO]" in out

    def test_json_format_emits_valid_json(self, monkeypatch, capsys):
        monkeypatch.setenv("LOG_FORMAT", "json")
        setup_logging("INFO")
        logging.getLogger("test").info("structured msg")
        out = capsys.readouterr().out.strip()
        assert out, "expected at least one log line"
        # Each line must be parseable JSON
        last = out.splitlines()[-1]
        payload = json.loads(last)
        assert payload["message"] == "structured msg"
        assert payload["level"] == "INFO"

    def test_structured_alias_also_emits_json(self, monkeypatch, capsys):
        monkeypatch.setenv("LOG_FORMAT", "structured")
        setup_logging("INFO")
        logging.getLogger("test").info("alias check")
        out = capsys.readouterr().out.strip().splitlines()[-1]
        payload = json.loads(out)
        assert payload["message"] == "alias check"

    def test_replaces_existing_handlers(self, monkeypatch):
        monkeypatch.delenv("LOG_FORMAT", raising=False)
        root = logging.getLogger()
        # Plant a dummy handler that should be cleared
        sentinel = logging.StreamHandler(io.StringIO())
        root.handlers = [sentinel]
        setup_logging("DEBUG")
        assert sentinel not in root.handlers
        assert root.level == logging.DEBUG
