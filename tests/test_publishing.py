import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from pipeline_doctor.incidents.models import Incident, IncidentStatus
from pipeline_doctor.incidents.repository import IncidentNotFoundError, IncidentRepository
from pipeline_doctor.jev.models import JevDecision, JevVerdict
from pipeline_doctor.memory.store import IncidentMemory, PastIncident
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


def test_approval_survives_a_memory_failure_without_reopening_the_pull_request(
    project: Path,
    repository: IncidentRepository,
    pending_incident: Incident,
    tmp_path: Path,
    recording_publisher: RecordingPublisher,
) -> None:
    # Setup
    class BrokenMemory(IncidentMemory):
        def remember(self, incident: PastIncident) -> None:
            raise OSError("disk full")

    repository.save(pending_incident)
    service = ApprovalService(repository, recording_publisher, BrokenMemory(tmp_path / "m.jsonl"), project)

    # Act
    approved = service.approve(pending_incident.id, "nader")

    # Assert
    assert approved.status is IncidentStatus.PR_OPENED
    assert repository.get(pending_incident.id).status is IncidentStatus.PR_OPENED
    with pytest.raises(IncidentApprovalError):
        service.approve(pending_incident.id, "nader")
    assert len(recording_publisher.opened) == 1


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


class _FakeGitHub:
    """Minimal in-memory GitHub Git Data API; `fail_on` makes matching requests answer 500."""

    def __init__(self, existing_branch: bool = False, open_pull_url: str | None = None, fail_on: str = "") -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.branch_exists = existing_branch
        self.open_pull_url = open_pull_url
        self.fail_on = fail_on

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        self.calls.append((request.method, path, body))
        if self.fail_on and path.endswith(self.fail_on):
            return httpx.Response(500, json={"message": "boom"})
        if request.method == "GET" and "/git/ref/heads/main" in path:
            return httpx.Response(200, json={"object": {"sha": "base-sha"}})
        if request.method == "GET" and path.endswith("/git/commits/base-sha"):
            return httpx.Response(200, json={"tree": {"sha": "base-tree"}})
        if path.endswith("/git/blobs"):
            return httpx.Response(201, json={"sha": f"blob-{len(self.calls)}"})
        if path.endswith("/git/trees"):
            return httpx.Response(201, json={"sha": "new-tree"})
        if path.endswith("/git/commits"):
            return httpx.Response(201, json={"sha": "new-commit"})
        if request.method == "POST" and path.endswith("/git/refs"):
            status = 422 if self.branch_exists else 201
            return httpx.Response(status, json={})
        if request.method == "PATCH":
            return httpx.Response(200, json={})
        if request.method == "GET" and path.endswith("/pulls"):
            return httpx.Response(200, json=[{"html_url": self.open_pull_url}] if self.open_pull_url else [])
        if request.method == "POST" and path.endswith("/pulls"):
            return httpx.Response(201, json={"html_url": "https://github.com/acme/dbt/pull/7"})
        return httpx.Response(404, json={})

    def requests(self, method: str, suffix: str) -> list[dict[str, object]]:
        return [body for call_method, path, body in self.calls if call_method == method and path.endswith(suffix)]


def _github_publisher(fake: _FakeGitHub, subdir: str = "") -> GitHubPublisher:
    client = httpx.Client(base_url="https://api.github.com", transport=httpx.MockTransport(fake.handle))
    return GitHubPublisher("acme/dbt", SecretStr("token"), "main", subdir, client=client)


def test_github_publisher_opens_a_draft_pull_request_from_a_single_commit(pending_incident: Incident) -> None:
    # Setup
    fake = _FakeGitHub()
    files = {"models/staging/stg_orders.sql": "select 1\n", "models/staging/_stg_models.yml": "version: 2\n"}

    # Act
    pull_request = _github_publisher(fake, "analytics").open_draft(pending_incident, files)

    # Assert
    (tree,) = fake.requests("POST", "/git/trees")
    (commit,) = fake.requests("POST", "/git/commits")
    (pull,) = fake.requests("POST", "/pulls")
    assert pull_request.url == "https://github.com/acme/dbt/pull/7"
    assert tree["base_tree"] == "base-tree"
    assert [entry["path"] for entry in tree["tree"]] == [  # type: ignore[attr-defined]
        "analytics/models/staging/_stg_models.yml",
        "analytics/models/staging/stg_orders.sql",
    ]
    assert commit["parents"] == ["base-sha"]
    assert fake.requests("POST", "/git/refs") == [
        {"ref": f"refs/heads/pipeline-doctor/{pending_incident.id}", "sha": "new-commit"}
    ]
    assert pull["draft"] is True


def test_github_publisher_resets_a_branch_left_by_a_failed_attempt(pending_incident: Incident) -> None:
    # Setup
    fake = _FakeGitHub(existing_branch=True)

    # Act
    pull_request = _github_publisher(fake).open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert pull_request.url == "https://github.com/acme/dbt/pull/7"
    assert ("PATCH", f"/repos/acme/dbt/git/refs/heads/pipeline-doctor/{pending_incident.id}") in [
        (method, path) for method, path, _ in fake.calls
    ]


def test_github_publisher_reuses_an_already_open_pull_request(pending_incident: Incident) -> None:
    # Setup
    fake = _FakeGitHub(open_pull_url="https://github.com/acme/dbt/pull/3")

    # Act
    pull_request = _github_publisher(fake).open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert pull_request.url == "https://github.com/acme/dbt/pull/3"
    assert fake.requests("POST", "/pulls") == []


def test_github_publisher_leaves_no_branch_when_a_blob_upload_fails(pending_incident: Incident) -> None:
    # Setup
    fake = _FakeGitHub(fail_on="/git/blobs")

    # Act
    with pytest.raises(PublishError, match="github POST"):
        _github_publisher(fake).open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})

    # Assert
    assert fake.requests("POST", "/git/refs") == []
    assert fake.requests("PATCH", f"pipeline-doctor/{pending_incident.id}") == []


def test_github_publisher_wraps_api_failures(pending_incident: Incident) -> None:
    # Setup
    fake = _FakeGitHub(fail_on="/git/ref/heads/main")

    # Act / Assert
    with pytest.raises(PublishError, match="github GET"):
        _github_publisher(fake).open_draft(pending_incident, {"models/staging/stg_orders.sql": "select 1\n"})


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


def test_approvals_from_separate_service_instances_open_only_one_pull_request(
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
    services = [
        ApprovalService(
            IncidentRepository(tmp_path / "incidents"), publisher, IncidentMemory(tmp_path / "m.jsonl"), project
        )
        for _ in range(4)
    ]

    def approve(service: ApprovalService) -> str:
        try:
            return service.approve(pending_incident.id, "nader").status
        except IncidentApprovalError:
            return "conflict"

    # Act
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(approve, services))

    # Assert
    assert publisher.opened == 1
    assert outcomes.count("conflict") == 3


def test_incident_save_leaves_no_temporary_file(repository: IncidentRepository, pending_incident: Incident) -> None:
    # Act
    repository.save(pending_incident)

    # Assert
    assert [path.suffix for path in (repository._directory).iterdir()] == [".json"]


def test_incident_save_from_concurrent_writers_leaves_valid_json(
    repository: IncidentRepository, pending_incident: Incident
) -> None:
    # Act
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: repository.save(pending_incident), range(32)))

    # Assert
    assert repository.get(pending_incident.id).id == pending_incident.id
    assert [path.suffix for path in repository._directory.iterdir()] == [".json"]


def test_incident_list_all_skips_a_corrupt_file(repository: IncidentRepository, pending_incident: Incident) -> None:
    # Setup
    repository.save(pending_incident)
    (repository._directory / "broken.json").write_text("{not json")

    # Act
    incidents = repository.list_all()

    # Assert
    assert [incident.id for incident in incidents] == [pending_incident.id]


@pytest.mark.parametrize("incident_id", ["١٢٣", "a/b", "..", "a b", ""])
def test_incident_get_rejects_ids_that_are_not_ascii_alphanumeric(
    repository: IncidentRepository, incident_id: str
) -> None:
    with pytest.raises(IncidentNotFoundError):
        repository.get(incident_id)


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
