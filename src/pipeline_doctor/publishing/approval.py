from pathlib import Path

from loguru import logger

from pipeline_doctor.agent.fixes import apply_edits
from pipeline_doctor.incidents.models import FixKind, Incident, IncidentStatus
from pipeline_doctor.incidents.repository import IncidentRepository
from pipeline_doctor.jev.models import JevDecision
from pipeline_doctor.memory.store import IncidentMemory, PastIncident
from pipeline_doctor.publishing.pull_requests import PullRequestPublisher


class IncidentApprovalError(RuntimeError):
    pass


class ApprovalService:
    """The only path to a write: a named human approves, then the fix is opened as a draft pull request."""

    def __init__(
        self, repository: IncidentRepository, publisher: PullRequestPublisher, memory: IncidentMemory, project_dir: Path
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._memory = memory
        self._project_dir = project_dir

    def approve(self, incident_id: str, approver: str) -> Incident:
        incident = self._get_pending(incident_id)
        if not approver.strip():
            raise IncidentApprovalError("an approver name is required")
        fix = incident.fix
        if fix is None or fix.kind is FixKind.OPERATIONAL or not fix.edits:
            resolved = incident.model_copy(
                update={"status": IncidentStatus.RESOLVED_OPERATIONALLY, "approved_by": approver}
            )
        else:
            verdict = incident.verdict
            if verdict is not None and verdict.decision is not JevDecision.PROCEED:
                raise IncidentApprovalError(f"incident {incident.id}: JEV decision is {verdict.decision}, not proceed")
            files = apply_edits(self._project_dir, fix.edits)
            pull_request = self._publisher.open_draft(incident, files)
            logger.info(f"incident {incident.id} approved by {approver}: {pull_request.url}")
            resolved = incident.model_copy(
                update={
                    "status": IncidentStatus.PR_OPENED,
                    "approved_by": approver,
                    "pull_request_url": pull_request.url,
                }
            )
        self._remember(resolved)
        return self._repository.save(resolved)

    def reject(self, incident_id: str, reviewer: str, reason: str) -> Incident:
        incident = self._get_pending(incident_id)
        rejected = incident.model_copy(
            update={"status": IncidentStatus.REJECTED, "approved_by": reviewer, "reviewer_note": reason}
        )
        return self._repository.save(rejected)

    def _get_pending(self, incident_id: str) -> Incident:
        incident = self._repository.get(incident_id)
        if incident.status is not IncidentStatus.AWAITING_APPROVAL:
            raise IncidentApprovalError(f"incident {incident_id} is {incident.status}, not awaiting approval")
        return incident

    def _remember(self, incident: Incident) -> None:
        if incident.diagnosis is None:
            return
        fix_title = incident.fix.title if incident.fix else "no fix"
        self._memory.remember(
            PastIncident(
                incident_id=incident.id,
                signature=incident.signature,
                category=incident.diagnosis.category,
                culprit=incident.diagnosis.culprit.describe(),
                resolution=f"{fix_title} (approved by {incident.approved_by})",
            )
        )
