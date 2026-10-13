import hashlib
import hmac
import time

import httpx
from loguru import logger
from pydantic import SecretStr

from pipeline_doctor.incidents.models import Incident

_MAX_REQUEST_AGE_SECONDS = 300


class SlackNotifier:
    """Posts incident summaries to a Slack incoming webhook; logs instead when no webhook is configured."""

    def __init__(self, webhook_url: SecretStr, public_base_url: str, client: httpx.Client | None = None) -> None:
        self._webhook_url = webhook_url.get_secret_value()
        self._public_base_url = public_base_url.rstrip("/")
        self._client = client or httpx.Client(timeout=10.0)

    def notify(self, incident: Incident) -> None:
        message = build_incident_message(incident, self._public_base_url)
        if not self._webhook_url:
            logger.info(f"slack not configured; incident {incident.id} summary: {message['text']}")
            return
        logger.info(f"post slack notification for incident {incident.id}")
        try:
            self._client.post(self._webhook_url, json=message).raise_for_status()
        except httpx.HTTPError:
            logger.exception(f"slack notification for incident {incident.id} failed")


def build_incident_message(incident: Incident, public_base_url: str) -> dict[str, object]:
    diagnosis = incident.diagnosis
    headline = (
        f"{diagnosis.category} on {diagnosis.culprit.describe()} ({diagnosis.confidence:.0%})"
        if diagnosis
        else f"diagnosis {incident.status}"
    )
    plain = diagnosis.plain_summary if diagnosis else (incident.error or "No diagnosis.")
    fix = f"*Proposed fix:* {incident.fix.title} (`{incident.fix.kind}`)" if incident.fix else "*Proposed fix:* none"
    text = f":stethoscope: {incident.event.describe()} failed: {headline}"
    return {
        "text": text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*{text}*"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": plain}},
            {"type": "section", "text": {"type": "mrkdwn", "text": fix}},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Review: {public_base_url}/incidents/{incident.id}/report · approve with "
                        f"`/doctor approve {incident.id}`",
                    }
                ],
            },
        ],
    }


def verify_slack_signature(signing_secret: SecretStr, timestamp: str, body: bytes, signature: str) -> bool:
    secret = signing_secret.get_secret_value()
    if not secret or not timestamp.isdigit() or abs(time.time() - int(timestamp)) > _MAX_REQUEST_AGE_SECONDS:
        return False
    basestring = f"v0:{timestamp}:".encode() + body
    expected = "v0=" + hmac.new(secret.encode(), basestring, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
