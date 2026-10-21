from datetime import UTC, datetime, timedelta

from loguru import logger

from pipeline_doctor.agent.factory import build_agent, build_memory
from pipeline_doctor.incidents.models import FailureEvent, Incident, IncidentStatus
from pipeline_doctor.incidents.repository import IncidentRepository
from pipeline_doctor.publishing.approval import ApprovalService
from pipeline_doctor.publishing.pull_requests import GitHubPublisher, LocalGitPublisher, PullRequestPublisher
from pipeline_doctor.settings import PublisherKind, Settings
from pipeline_doctor.vcs.repository import GitRepository


class IncidentIntakeLimitError(RuntimeError):
    pass


def build_publisher(settings: Settings) -> PullRequestPublisher:
    if settings.publisher is PublisherKind.GITHUB:
        return GitHubPublisher(
            settings.github_repository,
            settings.github_token,
            settings.github_base_branch,
            settings.github_project_subdir,
        )
    return LocalGitPublisher(GitRepository(settings.dbt_project_dir), settings.state_dir / "pull_requests")


class DoctorService:
    """Application service: opens incidents for failure events, diagnoses them, and routes approvals."""

    def __init__(self, settings: Settings, publisher: PullRequestPublisher | None = None) -> None:
        self._settings = settings
        self._repository = IncidentRepository(settings.state_dir / "incidents")
        self._memory = build_memory(settings)
        self._approvals = ApprovalService(
            self._repository, publisher or build_publisher(settings), self._memory, settings.dbt_project_dir
        )

    @property
    def repository(self) -> IncidentRepository:
        return self._repository

    @property
    def approvals(self) -> ApprovalService:
        return self._approvals

    def open_incident(self, event: FailureEvent) -> Incident:
        duplicate = self._find_duplicate(event)
        if duplicate is not None:
            logger.info(f"event already tracked as incident {duplicate.id}")
            return duplicate
        self._enforce_intake_limit()
        return self._repository.save(Incident(event=event))

    def diagnose(self, incident_id: str) -> Incident:
        pending = self._repository.get(incident_id)
        diagnosed = build_agent(self._settings, self._memory).diagnose(pending.event)
        return self._repository.save(diagnosed.model_copy(update={"id": pending.id, "created_at": pending.created_at}))

    def _enforce_intake_limit(self) -> None:
        window_start = datetime.now(UTC) - timedelta(hours=1)
        recent = sum(1 for incident in self._repository.list_all() if incident.created_at >= window_start)
        if recent >= self._settings.max_new_incidents_per_hour:
            raise IncidentIntakeLimitError(
                f"{recent} incidents opened in the last hour; the limit is {self._settings.max_new_incidents_per_hour}"
            )

    def _find_duplicate(self, event: FailureEvent) -> Incident | None:
        if not (event.dag_id and event.run_id and event.task_id):
            return None
        key = (event.dag_id, event.run_id, event.task_id, event.try_number)
        return next(
            (
                incident
                for incident in self._repository.list_all()
                if (incident.event.dag_id, incident.event.run_id, incident.event.task_id, incident.event.try_number)
                == key
                and incident.status is not IncidentStatus.FAILED
            ),
            None,
        )
