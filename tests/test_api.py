import hashlib
import hmac
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from pipeline_doctor.api.app import create_app
from pipeline_doctor.api.slack import verify_slack_signature
from pipeline_doctor.incidents.models import FailureEvent, Incident, IncidentStatus
from pipeline_doctor.publishing.pull_requests import PublishError
from pipeline_doctor.service import DoctorService
from pipeline_doctor.settings import Settings
from tests.conftest import RecordingPublisher

_TOKEN = "s3cret"
_REVIEWER_TOKEN = "lead-token"
_SIGNING_SECRET = "slack-secret"


class FakeAgent:
    def __init__(self, diagnosed: Incident) -> None:
        self._diagnosed = diagnosed

    def diagnose(self, event: FailureEvent) -> Incident:
        return self._diagnosed.model_copy(update={"event": event})


@pytest.fixture
def client(
    tmp_path: Path, pending_incident: Incident, monkeypatch: pytest.MonkeyPatch, recording_publisher: RecordingPublisher
) -> TestClient:
    project = tmp_path / "project" / "models" / "staging"
    project.mkdir(parents=True)
    (project / "stg_orders.sql").write_text("with source as (\n    select * from {{ source('raw', 'orders') }}\n)\n")
    settings = Settings(
        dbt_project_dir=tmp_path / "project",
        state_dir=tmp_path / "state",
        api_token=SecretStr(_TOKEN),
        reviewer_tokens={"lead": SecretStr(_REVIEWER_TOKEN)},
        slack_approvers=["lead"],
        slack_signing_secret=SecretStr(_SIGNING_SECRET),
        _env_file=None,
    )
    monkeypatch.setattr("pipeline_doctor.service.build_agent", lambda *_args: FakeAgent(pending_incident))
    return TestClient(create_app(settings, DoctorService(settings, publisher=recording_publisher)))


_AUTH = {"Authorization": f"Bearer {_TOKEN}"}
_REVIEWER_AUTH = {"Authorization": f"Bearer {_REVIEWER_TOKEN}"}
_CALLBACK = {"dag_id": "shop", "task_id": "dbt_build", "run_id": "scheduled__1", "try_number": 1}


def test_airflow_webhook_requires_token(client: TestClient) -> None:
    assert client.post("/webhooks/airflow", json=_CALLBACK).status_code == 401


def test_airflow_webhook_diagnoses_in_background(client: TestClient) -> None:
    # Act
    accepted = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()
    incident = client.get(f"/incidents/{accepted['incident_id']}", headers=_AUTH).json()

    # Assert
    assert incident["status"] == IncidentStatus.AWAITING_APPROVAL
    assert incident["event"]["task_id"] == "dbt_build"


def test_airflow_webhook_deduplicates_retries_of_the_same_event(client: TestClient) -> None:
    # Act
    first = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()
    second = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()

    # Assert
    assert first["incident_id"] == second["incident_id"]


def test_approve_opens_pr_and_second_approval_conflicts(client: TestClient) -> None:
    # Setup
    incident_id = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()["incident_id"]

    # Act
    first = client.post(f"/incidents/{incident_id}/approve", headers=_REVIEWER_AUTH)
    second = client.post(f"/incidents/{incident_id}/approve", headers=_REVIEWER_AUTH)

    # Assert
    assert first.json()["status"] == IncidentStatus.PR_OPENED
    assert first.json()["approved_by"] == "lead"
    assert second.status_code == 409


def test_failed_publish_returns_502_and_stays_retryable(
    client: TestClient, recording_publisher: RecordingPublisher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Setup
    incident_id = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()["incident_id"]
    monkeypatch.setattr(
        recording_publisher, "open_draft", lambda *_args: (_ for _ in ()).throw(PublishError("github down"))
    )

    # Act
    failed = client.post(f"/incidents/{incident_id}/approve", headers=_REVIEWER_AUTH)
    status = client.get(f"/incidents/{incident_id}", headers=_AUTH).json()["status"]

    # Assert
    assert failed.status_code == 502
    assert status == IncidentStatus.AWAITING_APPROVAL


def test_service_token_cannot_approve(client: TestClient) -> None:
    # Setup
    incident_id = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()["incident_id"]

    # Act
    response = client.post(f"/incidents/{incident_id}/approve", headers=_AUTH)

    # Assert
    assert response.status_code == 401


def test_slack_approve_requires_an_allowlisted_user(client: TestClient) -> None:
    # Setup
    incident_id = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()["incident_id"]
    body = f"text=approve+{incident_id}&user_name=intern".encode()

    # Act
    response = client.post("/slack/commands", content=body, headers=_slack_headers(body))

    # Assert
    assert "cannot approve" in response.json()["text"]
    assert client.get(f"/incidents/{incident_id}", headers=_AUTH).json()["status"] == IncidentStatus.AWAITING_APPROVAL


def test_unknown_incident_returns_404(client: TestClient) -> None:
    assert client.get("/incidents/doesnotexist", headers=_AUTH).status_code == 404


def _slack_headers(body: bytes, secret: str = _SIGNING_SECRET) -> dict[str, str]:
    timestamp = str(int(time.time()))
    digest = hmac.new(secret.encode(), f"v0:{timestamp}:".encode() + body, hashlib.sha256).hexdigest()
    return {
        "X-Slack-Request-Timestamp": timestamp,
        "X-Slack-Signature": f"v0={digest}",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def test_slack_explain_command_returns_plain_language_summary(client: TestClient, pending_incident: Incident) -> None:
    # Setup
    incident_id = client.post("/webhooks/airflow", json=_CALLBACK, headers=_AUTH).json()["incident_id"]
    body = f"text=explain+{incident_id}&user_name=analyst".encode()

    # Act
    response = client.post("/slack/commands", content=body, headers=_slack_headers(body))

    # Assert
    assert response.json()["text"] == pending_incident.diagnosis.plain_summary


def test_slack_command_with_bad_signature_is_rejected(client: TestClient) -> None:
    body = b"text=approve+abc"
    assert client.post("/slack/commands", content=body, headers=_slack_headers(body, "wrong")).status_code == 401


def test_verify_slack_signature_rejects_replayed_requests() -> None:
    # Setup
    old = str(int(time.time()) - 3_600)
    signature = "v0=" + hmac.new(b"k", f"v0:{old}:".encode() + b"x", hashlib.sha256).hexdigest()

    # Act / Assert
    assert verify_slack_signature(SecretStr("k"), old, b"x", signature) is False
