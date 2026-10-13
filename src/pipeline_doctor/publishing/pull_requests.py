import base64
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
    return f"[Pipeline Doctor] {title}"


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
    """Opens a draft pull request through the GitHub REST API using a token scoped to one repository."""

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
        logger.info(f"create github branch {self._repository}:{branch}")
        base = self._request("GET", f"{repo}/git/ref/heads/{quote(self._base_branch)}").json()
        self._request("POST", f"{repo}/git/refs", json={"ref": f"refs/heads/{branch}", "sha": base["object"]["sha"]})
        for path, content in sorted(files.items()):
            self._put_file(repo, branch, f"{self._prefix}{path}", content, title)
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

    def _put_file(self, repo: str, branch: str, path: str, content: str, message: str) -> None:
        url = f"{repo}/contents/{quote(path)}"
        existing = self._client.get(url, params={"ref": branch})
        payload: dict[str, object] = {
            "message": message,
            "content": base64.b64encode(content.encode()).decode(),
            "branch": branch,
        }
        if existing.status_code == httpx.codes.OK:
            payload["sha"] = existing.json()["sha"]
        logger.info(f"commit github file {path}")
        self._request("PUT", url, json=payload)

    def _request(self, method: str, url: str, json: dict[str, object] | None = None) -> httpx.Response:
        response = self._client.request(method, url, json=json)
        response.raise_for_status()
        return response
