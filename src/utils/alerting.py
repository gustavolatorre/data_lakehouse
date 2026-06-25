"""Opt-in operational alerting.

``send_alert`` POSTs a short message to a Slack incoming webhook when
``ALERT_WEBHOOK_URL`` is configured; otherwise it logs the alert and returns.
It never raises — a webhook outage must not fail the DAG or the Spark job.

This mirrors the OpenLineage opt-in switch (empty URL = no external call), so
the bundled stack keeps working with zero extra configuration: by default an
alert is just a structured WARNING in the task log. Point ``ALERT_WEBHOOK_URL``
at a Slack incoming webhook to start delivering failure / quarantine alerts to
a channel. The generic ``(title, body, severity)`` signature means a different
sink (SMTP, PagerDuty) can be swapped in later without touching call sites.
"""

import logging

import requests

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 10


def send_alert(title: str, body: str, *, severity: str = "error") -> bool:
    """Deliver an operational alert to the configured Slack webhook.

    Args:
        title: Short headline (e.g. ``"BRONZE/SILVER task failed"``).
        body: Detail line (dag/task/date/error, or quarantine counts).
        severity: Free-form label surfaced in the message ("error", "warning").

    Returns:
        ``True`` if the alert was delivered over HTTP; ``False`` if it was
        log-only (no webhook configured) or delivery failed. Never raises.
    """
    message = f"[{severity.upper()}] {title} — {body}"
    webhook_url = get_settings().alert_webhook_url

    if not webhook_url:
        logger.warning("ALERT (log-only; set ALERT_WEBHOOK_URL to deliver): %s", message)
        return False

    try:
        response = requests.post(
            webhook_url,
            json={"text": f":rotating_light: {message}"},
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("Alert delivery failed; original alert: %s", message)
        return False

    logger.info("Alert delivered to webhook: %s", message)
    return True
