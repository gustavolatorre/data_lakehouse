"""Centralized logging configuration with optional JSON structured output.

By default, logs are formatted as plain text for local development.
Set LOG_FORMAT=json (or LOG_FORMAT=structured) in the environment to emit
single-line JSON records suitable for Elasticsearch, CloudWatch, Datadog,
or any log aggregator that parses JSON.

Usage at process entry points (e.g. Spark job main, Lambda handler):

    from src.utils.logging_config import setup_logging
    setup_logging()
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects.

    Includes timestamp, level, logger name, message, and any
    extras attached via `logger.info("msg", extra={...})`. Exceptions
    are flattened into an `exception` field with the full traceback.
    """

    RESERVED = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "asctime",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key not in self.RESERVED and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: str | int = "INFO") -> None:
    """Configure the root logger based on the LOG_FORMAT environment variable.

    Args:
        level: Logging level name or numeric value. Defaults to INFO.
    """
    log_format = os.getenv("LOG_FORMAT", "text").lower()

    handler = logging.StreamHandler(sys.stdout)

    if log_format in ("json", "structured"):
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
