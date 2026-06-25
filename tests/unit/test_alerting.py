"""Unit tests for the opt-in alerting helper."""

from types import SimpleNamespace

import pytest
import requests
import responses

from src.utils import alerting

_WEBHOOK = "https://hooks.slack.test/services/T000/B000/xxx"  # pragma: allowlist secret


@pytest.fixture
def _no_webhook(monkeypatch):
    """Settings with an empty ALERT_WEBHOOK_URL (default, log-only)."""
    monkeypatch.setattr(alerting, "get_settings", lambda: SimpleNamespace(alert_webhook_url=""))


@pytest.fixture
def _with_webhook(monkeypatch):
    """Settings pointing at a (fake) Slack webhook."""
    monkeypatch.setattr(alerting, "get_settings", lambda: SimpleNamespace(alert_webhook_url=_WEBHOOK))


class TestSendAlert:
    def test_log_only_when_no_webhook(self, _no_webhook):
        """With no webhook configured, returns False and makes no HTTP call."""
        assert alerting.send_alert("title", "body") is False

    @responses.activate
    def test_posts_when_webhook_set(self, _with_webhook):
        """With a webhook configured, POSTs the message and returns True."""
        responses.add(responses.POST, _WEBHOOK, status=200)

        assert alerting.send_alert("task failed", "dag=x task=y", severity="error") is True

        assert len(responses.calls) == 1
        sent = responses.calls[0].request.body
        sent = sent.decode("utf-8") if isinstance(sent, bytes | bytearray) else sent
        assert ":rotating_light:" in sent

    @responses.activate
    def test_swallows_http_error(self, _with_webhook):
        """A 5xx from the webhook is swallowed — returns False, never raises."""
        responses.add(responses.POST, _WEBHOOK, status=500)

        assert alerting.send_alert("title", "body") is False

    def test_swallows_connection_error(self, _with_webhook, monkeypatch):
        """A transport error is swallowed — returns False, never raises."""

        def _boom(*_args, **_kwargs):
            raise requests.exceptions.ConnectionError("down")

        monkeypatch.setattr(alerting.requests, "post", _boom)
        assert alerting.send_alert("title", "body") is False
