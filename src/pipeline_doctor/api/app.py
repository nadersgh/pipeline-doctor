import hmac
from typing import Annotated
from urllib.parse import parse_qs

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse
from loguru import logger
from pydantic import BaseModel

from pipeline_doctor.agent.fixes import FixApplicationError
from pipeline_doctor.airflow.events import AirflowFailureCallback, DbtFailureNotification
from pipeline_doctor.api.slack import SlackNotifier, verify_slack_signature
from pipeline_doctor.incidents.models import FailureEvent, Incident
from pipeline_doctor.incidents.report import render_incident_report
from pipeline_doctor.incidents.repository import IncidentNotFoundError
from pipeline_doctor.publishing.approval import IncidentApprovalError
from pipeline_doctor.publishing.pull_requests import PublishError
from pipeline_doctor.service import DoctorService, IncidentIntakeLimitError
from pipeline_doctor.settings import Settings


class IncidentAccepted(BaseModel):
    incident_id: str
    status: str


class RejectionRequest(BaseModel):
    reason: str


def create_app(settings: Settings | None = None, service: DoctorService | None = None) -> FastAPI:
    settings = settings or Settings()
    service = service or DoctorService(settings)
    notifier = SlackNotifier(settings.slack_webhook_url, settings.public_base_url)
    app = FastAPI(title="Pipeline Doctor", version="0.1.0")

    @app.exception_handler(IncidentNotFoundError)
    def incident_not_found(_: Request, error: IncidentNotFoundError) -> JSONResponse:
        return JSONResponse({"detail": f"incident {error} not found"}, status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(IncidentApprovalError)
    @app.exception_handler(FixApplicationError)
    def approval_conflict(_: Request, error: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_409_CONFLICT)

    @app.exception_handler(IncidentIntakeLimitError)
    def intake_limited(_: Request, error: IncidentIntakeLimitError) -> JSONResponse:
        logger.warning(f"incident intake limited: {error}")
        return JSONResponse(
            {"detail": f"too many new incidents: {error}"},
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={"Retry-After": "3600"},
        )

    @app.exception_handler(PublishError)
    def publish_failed(_: Request, error: PublishError) -> JSONResponse:
        logger.error(f"publish failed: {error}")
        return JSONResponse(
            {"detail": f"publishing failed, retry approval: {error}"}, status_code=status.HTTP_502_BAD_GATEWAY
        )

    reviewer_tokens = {name: token.get_secret_value() for name, token in settings.reviewer_tokens.items()}
    service_token = settings.api_token.get_secret_value()
    if not service_token:
        if not settings.allow_unauthenticated:
            raise RuntimeError(
                "PIPELINE_DOCTOR_API_TOKEN is empty: set it, or set PIPELINE_DOCTOR_ALLOW_UNAUTHENTICATED=true "
                "to serve webhooks and incident reads without authentication"
            )
        logger.warning("PIPELINE_DOCTOR_ALLOW_UNAUTHENTICATED is set: webhooks and incident reads are open")

    def require_token(authorization: Annotated[str, Header()] = "") -> None:
        accepted = [token for token in [service_token, *reviewer_tokens.values()] if token]
        if service_token and not any(_matches_bearer(authorization, token) for token in accepted):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing bearer token")

    def require_reviewer(authorization: Annotated[str, Header()] = "") -> str:
        if not reviewer_tokens:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "approvals are disabled: configure reviewer tokens")
        reviewer = next(
            (name for name, token in reviewer_tokens.items() if _matches_bearer(authorization, token)), None
        )
        if reviewer is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "a reviewer token is required to approve or reject")
        return reviewer

    def diagnose_and_notify(incident_id: str) -> None:
        notifier.notify(service.diagnose(incident_id))

    def accept(event: FailureEvent, background: BackgroundTasks) -> IncidentAccepted:
        incident = service.open_incident(event)
        if incident.diagnosis is None:
            background.add_task(diagnose_and_notify, incident.id)
        return IncidentAccepted(incident_id=incident.id, status=incident.status)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/webhooks/airflow", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(require_token)])
    def airflow_failure(payload: AirflowFailureCallback, background: BackgroundTasks) -> IncidentAccepted:
        return accept(payload.to_event(), background)

    @app.post("/webhooks/dbt", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(require_token)])
    def dbt_failure(payload: DbtFailureNotification, background: BackgroundTasks) -> IncidentAccepted:
        return accept(payload.to_event(), background)

    @app.get("/incidents", dependencies=[Depends(require_token)])
    def list_incidents() -> list[Incident]:
        return service.repository.list_all()

    @app.get("/incidents/{incident_id}", dependencies=[Depends(require_token)])
    def get_incident(incident_id: str) -> Incident:
        return service.repository.get(incident_id)

    @app.get("/incidents/{incident_id}/report", response_class=PlainTextResponse, dependencies=[Depends(require_token)])
    def get_report(incident_id: str) -> str:
        return render_incident_report(service.repository.get(incident_id))

    @app.post("/incidents/{incident_id}/approve")
    def approve(incident_id: str, reviewer: Annotated[str, Depends(require_reviewer)]) -> Incident:
        return service.approvals.approve(incident_id, reviewer)

    @app.post("/incidents/{incident_id}/reject")
    def reject(
        incident_id: str, request: RejectionRequest, reviewer: Annotated[str, Depends(require_reviewer)]
    ) -> Incident:
        return service.approvals.reject(incident_id, reviewer, request.reason)

    @app.post("/slack/commands")
    async def slack_command(
        request: Request,
        x_slack_request_timestamp: Annotated[str, Header()] = "",
        x_slack_signature: Annotated[str, Header()] = "",
    ) -> dict[str, str]:
        body = await request.body()
        if not verify_slack_signature(
            settings.slack_signing_secret, x_slack_request_timestamp, body, x_slack_signature
        ):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid Slack signature")
        form = {key: values[0] for key, values in parse_qs(body.decode()).items()}
        return {"response_type": "ephemeral", "text": _handle_slack_command(service, form, settings.slack_approvers)}

    return app


def _matches_bearer(authorization: str, token: str) -> bool:
    return hmac.compare_digest(authorization.encode(), f"Bearer {token}".encode())


def _handle_slack_command(service: DoctorService, form: dict[str, str], approvers: list[str]) -> str:
    words = form.get("text", "").split()
    if len(words) != 2 or words[0] not in {"explain", "approve", "status"}:
        return "Usage: `/doctor explain <incident-id>`, `/doctor status <incident-id>`, `/doctor approve <incident-id>`"
    action, incident_id = words
    try:
        if action == "approve":
            user_id = form.get("user_id", "")
            if not user_id or user_id not in approvers:
                return "You cannot approve fixes; ask an approver listed in PIPELINE_DOCTOR_SLACK_APPROVERS."
            user_name = form.get("user_name", "")
            incident = service.approvals.approve(incident_id, f"{user_name} {user_id} (slack)".strip())
            return f"Approved. {incident.pull_request_url or 'Marked as resolved operationally.'}"
        incident = service.repository.get(incident_id)
    except (IncidentNotFoundError, IncidentApprovalError, FixApplicationError, PublishError) as error:
        return f"Could not {action} {incident_id}: {error}"
    if action == "status":
        return f"Incident {incident.id} is {incident.status}."
    return incident.diagnosis.plain_summary if incident.diagnosis else f"Incident {incident.id} is {incident.status}."
