from pathlib import Path
from typing import Protocol
from urllib.parse import quote

import httpx
from loguru import logger
from pydantic import BaseModel, SecretStr

from pipeline_doctor.incidents.models import Incident
from pipeline_doctor.incidents.report import render_incident_report
from pipeline_doctor.vcs.repository import GitRepository

_PR_FOOTER = (
    "\n---\n_Opened as a **draft** by Pipeline Doctor. It was generated from read-only evidence and must be "
    "reviewed by a human before merging._\n"
)

_MAX_TITLE_CHARS = 100


class PublishError(RuntimeError):
    """The code host refused or failed a publish step; the incident stays pending so approval can be retried."""


class PullRequest(BaseModel):
    url: str
    branch: str
    title: str


class PullRequestPublisher(Protocol):
    def open_draft(self, incident: Incident, files: dict[str, str]) -> PullRequest: ...


def branch_name(incident: Incident) -> str:
    return f"pipeline-doctor/{incident.id}"


def pull_request_title(incident: Incident) -> str:
    title = incident.fix.title if incident.fix else "Proposed fix"
    return f"[Pipeline Doctor] {_sanitise_title(title)}"


def _sanitise_title(title: str) -> str:
    """Single line of printable characters, so model output cannot inject lines into a title or commit subject."""
    single_line = " ".join("".join(char for char in title if char.isprintable() or char.isspace()).split())
    if len(single_line) <= _MAX_TITLE_CHARS:
        return single_line or "Proposed fix"
    return f"{single_line[: _MAX_TITLE_CHARS - 1].rstrip()}…"


class LocalGitPublisher:
    """Commits the fix onto a new local branch (working tree untouched) and writes the PR description to disk."""

    def __init__(self, repository: GitRepository, output_dir: Path) -> None:
        self._repository = repository
        self._output_dir = output_dir

    def open_draft(self, incident: Incident, files: dict[str, str]) -> PullRequest:
        branch = branch_name(incident)
        title = pull_request_title(incident)
        self._repository.create_branch_commit(branch, f"{title}\n\nIncident {incident.id}", files)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        description = self._output_dir / f"{incident.id}.md"
        logger.info(f"write pull request description {description}")
        description.write_text(f"# {title}\n\n{render_incident_report(incident)}{_PR_FOOTER}")
        return PullRequest(url=description.resolve().as_uri(), branch=branch, title=title)


class GitHubPublisher:
    """Opens a draft pull request through the GitHub REST API using a token scoped to one repository.

    All files land in a single commit that is attached to the branch last, so a failed publish leaves no partial branch.
    """

    def __init__(
        self,
        repository: str,
        token: SecretStr,
        base_branch: str,
        project_subdir: str = "",
        client: httpx.Client | None = None,
    ) -> None:
        self._repository = repository
        self._base_branch = base_branch
        self._prefix = f"{project_subdir.strip('/')}/" if project_subdir.strip("/") else ""
        self._client = client or httpx.Client(
            base_url="https://api.github.com",
            headers={
                "Authorization": f"Bearer {token.get_secret_value()}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30.0,
        )

    def open_draft(self, incident: Incident, files: dict[str, str]) -> PullRequest:
        branch = branch_name(incident)
        title = pull_request_title(incident)
        repo = f"/repos/{self._repository}"
        base_sha = self._request("GET", f"{repo}/git/ref/heads/{quote(self._base_branch)}").json()["object"]["sha"]
        commit_sha = self._create_commit(repo, base_sha, files, title)
        self._point_branch_at(repo, branch, commit_sha)
        existing = self._open_pull_request_url(repo, branch)
        if existing is not None:
            logger.info(f"github pull request for {self._repository}:{branch} already open")
            return PullRequest(url=existing, branch=branch, title=title)
        logger.info(f"open github draft pull request {self._repository}:{branch}")
        pull = self._request(
            "POST",
            f"{repo}/pulls",
            json={
                "title": title,
                "head": branch,
                "base": self._base_branch,
                "body": render_incident_report(incident) + _PR_FOOTER,
                "draft": True,
            },
        ).json()
        return PullRequest(url=pull["html_url"], branch=branch, title=title)

    def _create_commit(self, repo: str, base_sha: str, files: dict[str, str], message: str) -> str:
        """Builds one commit from all files; nothing is visible on the branch until it is pointed at the commit."""
        logger.info(f"create github commit {self._repository} {len(files)} files")
        base_tree = self._request("GET", f"{repo}/git/commits/{base_sha}").json()["tree"]["sha"]
        entries = [
            {"path": f"{self._prefix}{path}", "mode": "100644", "type": "blob", "sha": self._create_blob(repo, content)}
            for path, content in sorted(files.items())
        ]
        tree = self._request("POST", f"{repo}/git/trees", json={"base_tree": base_tree, "tree": entries}).json()
        commit = self._request(
            "POST", f"{repo}/git/commits", json={"message": message, "tree": tree["sha"], "parents": [base_sha]}
        ).json()
        return str(commit["sha"])

    def _create_blob(self, repo: str, content: str) -> str:
        blob = self._request("POST", f"{repo}/git/blobs", json={"content": content, "encoding": "utf-8"}).json()
        return str(blob["sha"])

    def _point_branch_at(self, repo: str, branch: str, sha: str) -> None:
        """Creates the branch, or force-resets one left behind by an earlier attempt."""
        response = self._client.request("POST", f"{repo}/git/refs", json={"ref": f"refs/heads/{branch}", "sha": sha})
        if response.status_code == httpx.codes.UNPROCESSABLE_ENTITY:
            logger.info(f"reset existing github branch {branch}")
            self._request("PATCH", f"{repo}/git/refs/heads/{quote(branch)}", json={"sha": sha, "force": True})
            return
        self._raise_for_status(response)

    def _open_pull_request_url(self, repo: str, branch: str) -> str | None:
        owner = self._repository.split("/")[0]
        response = self._request("GET", f"{repo}/pulls", params={"head": f"{owner}:{branch}", "state": "open"})
        pulls = response.json()
        return pulls[0]["html_url"] if pulls else None

    def _request(
        self,
        method: str,
        url: str,
        json: dict[str, object] | None = None,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            response = self._client.request(method, url, json=json, params=params)
        except httpx.HTTPError as error:
            raise PublishError(f"github {method} {url} failed: {error}") from error
        return self._raise_for_status(response)

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> httpx.Response:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            raise PublishError(f"github {error.request.method} {error.request.url.path} failed: {error}") from error
        return response
