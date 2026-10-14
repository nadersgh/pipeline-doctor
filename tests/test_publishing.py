import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from pipeline_doctor.incidents.models import Incident, IncidentStatus
from pipeline_doctor.incidents.repository import IncidentRepository
from pipeline_doctor.jev.models import JevDecision, JevVerdict
from pipeline_doctor.memory.store import IncidentMemory
from pipeline_doctor.publishing.approval import ApprovalService, IncidentApprovalError
from pipeline_doctor.publishing.pull_requests import GitHubPublisher, LocalGitPublisher, PublishError, PullRequest
from pipeline_doctor.settings import PublisherKind, Settings
from pipeline_doctor.vcs.repository import GitRepository
from tests.conftest import RecordingPublisher


@pytest.fixture
def project(tmp_path: Path) -> Path:
    model = tmp_path / "project" / "models" / "staging" / "stg_orders.sql"
    model.parent.mkdir(parents=True)
    model.write_text("with source as (\n    select * from {{ source('raw', 'orders') }}\n)\nselect * from source\n")
    return tmp_path / "project"


@pytest.fixture
def repository(tmp_path: Path) -> IncidentRepository:
    return IncidentRepository(tmp_path / "incidents")


def test_approval_refuses_a_code_change_jev_did_not_clear(
    project: Path,
    repository: IncidentRepository,
    pending_incident: Incident,
    tmp_path: Path,
    recording_publisher: RecordingPublisher,
) -> None:
    # Setup
    verdict = JevVerdict(decision=JevDecision.REVISE_FIX, score=0.5)
    repository.save(pending_incident.model_copy(update={"verdict": verdict}))
    service = ApprovalService(repository, recording_publisher, IncidentMemory(tmp_path / "memory.jsonl"), project)

    # Act / Assert
    with pytest.raises(IncidentApprovalError, match="JEV decision is revise_fix"):
        service.approve(pending_incident.id, "nader")
    assert recording_publisher.opened == []


def test_approval_opens_draft_pr_and_records_memory(
    project: Path,
    repository: IncidentRepository,
    pending_incident: Incident,
    tmp_path: Path,
    recording_publisher: RecordingPublisher,
) -> None:
    # Setup
    publisher = recording_publisher
    memory = IncidentMemory(tmp_path / "memory.jsonl")
    repository.save(pending_incident)
    service = ApprovalService(repository, publisher, memory, project)

    # Act
    approved = service.approve(pending_incident.id, "nader")

    # Assert
    assert approved.status is IncidentStatus.PR_OPENED
    assert approved.pull_request_url == "https://example.test/pr/1"
    assert "rename (order_status as status)" in publisher.opened[0]["models/staging/stg_orders.sql"]
    assert memory.search(pending_incident.signature)[0].incident.incident_id == pending_incident.id


def test_approval_refuses_incidents_not_awaiting_review(
    project: Path,
    repository: IncidentRepository,
    pending_incident: Incident,
    tmp_path: Path,
    recording_publisher: RecordingPublisher,
) -> None:
    # Setup
    repository.save(pending_incident.model_copy(update={"status": IncidentStatus.REJECTED}))
    service = ApprovalService(repository, recording_publisher, IncidentMemory(tmp_path / "m.jsonl"), project)

    # Act / Assert
    with pytest.raises(IncidentApprovalError):
        service.approve(pending_incident.id, "nader")


def test_rejection_records_reviewer_note_without_publishing(
    project: Path,
    repository: IncidentRepository,
    pending_incident: Incident,
    tmp_path: Path,
    recording_publisher: RecordingPublisher,
) -> None:
    # Setup
    publisher = recording_publisher
    repository.save(pending_incident)
    service = ApprovalService(repository, publisher, IncidentMemory(tmp_path / "m.jsonl"), project)

    # Act
    rejected = service.reject(pending_incident.id, "lead", "rename is temporary")

    # Assert
    assert rejected.status is IncidentStatus.REJECTED
    assert rejected.reviewer_note == "rename is temporary"
    assert publisher.opened == []


def test_local_git_publisher_commits_to_a_branch(project: Path, pending_incident: Incident, tmp_path: Path) -> None:
    # Setup
    git = GitRepository(project)
    git.init()
    git.commit_all("initial")
    publisher = LocalGitPublisher(git, tmp_path / "prs")

    # Act
    pull_request = publisher.open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert git.show_file(pull_request.branch, "models/staging/stg_orders.sql") == "select 1\n"
    assert (tmp_path / "prs" / f"{pending_incident.id}.md").read_text().startswith("# [Pipeline Doctor]")


def test_github_publisher_opens_a_draft_pull_request(pending_incident: Incident) -> None:
    # Setup
    calls: list[tuple[str, str, dict[str, object]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        calls.append((request.method, request.url.path, body))
        if request.method == "GET" and "/git/ref/" in request.url.path:
            return httpx.Response(200, json={"object": {"sha": "base-sha"}})
        if request.method == "GET" and "/contents/" in request.url.path:
            return httpx.Response(200, json={"sha": "file-sha"})
        if request.method == "GET" and request.url.path.endswith("/pulls"):
            return httpx.Response(200, json=[])
        if request.url.path.endswith("/pulls"):
            return httpx.Response(201, json={"html_url": "https://github.com/acme/dbt/pull/7"})
        return httpx.Response(201, json={})

    client = httpx.Client(base_url="https://api.github.com", transport=httpx.MockTransport(handler))
    publisher = GitHubPublisher("acme/dbt", SecretStr("token"), "main", "analytics", client=client)

    # Act
    pull_request = publisher.open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    put = next(call for call in calls if call[0] == "PUT")
    pull = next(call for call in calls if call[0] == "POST" and call[1].endswith("/pulls"))
    assert pull_request.url == "https://github.com/acme/dbt/pull/7"
    assert put[1] == "/repos/acme/dbt/contents/analytics/models/staging/stg_orders.sql"
    assert put[2]["sha"] == "file-sha"
    assert pull[2]["draft"] is True


def _github_client(handler: httpx.MockTransport | object) -> httpx.Client:
    return httpx.Client(base_url="https://api.github.com", transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


def test_github_publisher_resets_a_branch_left_by_a_failed_attempt(pending_incident: Incident) -> None:
    # Setup
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET" and "/git/ref/" in request.url.path:
            return httpx.Response(200, json={"object": {"sha": "base-sha"}})
        if request.method == "POST" and request.url.path.endswith("/git/refs"):
            return httpx.Response(422, json={"message": "Reference already exists"})
        if request.method == "GET" and request.url.path.endswith("/pulls"):
            return httpx.Response(200, json=[])
        if request.method == "GET":
            return httpx.Response(404, json={})
        if request.url.path.endswith("/pulls"):
            return httpx.Response(201, json={"html_url": "https://github.com/acme/dbt/pull/8"})
        return httpx.Response(200, json={})

    publisher = GitHubPublisher("acme/dbt", SecretStr("token"), "main", client=_github_client(handler))

    # Act
    pull_request = publisher.open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert pull_request.url == "https://github.com/acme/dbt/pull/8"
    assert ("PATCH", f"/repos/acme/dbt/git/refs/heads/pipeline-doctor/{pending_incident.id}") in calls


def test_github_publisher_reuses_an_already_open_pull_request(pending_incident: Incident) -> None:
    # Setup
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET" and "/git/ref/" in request.url.path:
            return httpx.Response(200, json={"object": {"sha": "base-sha"}})
        if request.method == "GET" and request.url.path.endswith("/pulls"):
            return httpx.Response(200, json=[{"html_url": "https://github.com/acme/dbt/pull/3"}])
        if request.method == "GET":
            return httpx.Response(404, json={})
        return httpx.Response(201, json={})

    publisher = GitHubPublisher("acme/dbt", SecretStr("token"), "main", client=_github_client(handler))

    # Act
    pull_request = publisher.open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert pull_request.url == "https://github.com/acme/dbt/pull/3"
    assert ("POST", "/repos/acme/dbt/pulls") not in calls


def test_github_publisher_wraps_api_failures(pending_incident: Incident) -> None:
    # Setup
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"message": "boom"})

    publisher = GitHubPublisher("acme/dbt", SecretStr("token"), "main", client=_github_client(handler))

    # Act / Assert
    with pytest.raises(PublishError, match="github GET"):
        publisher.open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})


def test_concurrent_approvals_open_only_one_pull_request(
    project: Path, repository: IncidentRepository, pending_incident: Incident, tmp_path: Path
) -> None:
    # Setup
    class SlowPublisher:
        def __init__(self) -> None:
            self.opened = 0

        def open_draft(self, incident: Incident, files: dict[str, str]) -> PullRequest:
            self.opened += 1
            time.sleep(0.05)
            return PullRequest(url="https://example.test/pr/1", branch="b", title="t")

    publisher = SlowPublisher()
    repository.save(pending_incident)
    service = ApprovalService(repository, publisher, IncidentMemory(tmp_path / "m.jsonl"), project)

    def approve() -> str:
        try:
            return service.approve(pending_incident.id, "nader").status
        except IncidentApprovalError:
            return "conflict"

    # Act
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(lambda _: approve(), range(4)))

    # Assert
    assert publisher.opened == 1
    assert outcomes.count("conflict") == 3


def test_incident_save_leaves_no_temporary_file(repository: IncidentRepository, pending_incident: Incident) -> None:
    # Act
    repository.save(pending_incident)

    # Assert
    assert [path.suffix for path in (repository._directory).iterdir()] == [".json"]


def test_github_publisher_setting_requires_repository_and_token() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="github_repository and github_token"):
        Settings(publisher=PublisherKind.GITHUB, _env_file=None)
    assert Settings(
        publisher=PublisherKind.GITHUB, github_repository="acme/dbt", github_token=SecretStr("t"), _env_file=None
    )


def test_local_git_publisher_commits_without_a_configured_identity(
    project: Path, pending_incident: Incident, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Setup
    git = GitRepository(project)
    git.init()
    git.commit_all("initial")
    for key in ("user.name", "user.email"):
        git._run(["config", "--unset", key])
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    publisher = LocalGitPublisher(git, tmp_path / "prs")

    # Act
    pull_request = publisher.open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert git.show_file(pull_request.branch, "models/staging/stg_orders.sql") == "select 1\n"
