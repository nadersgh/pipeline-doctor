import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from loguru import logger
from pydantic import BaseModel

_SAFE_REF = re.compile(r"^[\w./@^~:-]+$")
_FIELD_SEPARATOR = "\x1f"
_RECORD_SEPARATOR = "\x1e"


class GitCommandError(RuntimeError):
    pass


class CommitInfo(BaseModel):
    sha: str
    author: str
    authored_at: datetime
    message: str
    files: list[str]


class GitRepository:
    """Thin wrapper over the git CLI using fixed argument vectors (never a shell)."""

    def __init__(self, root: Path) -> None:
        self._root = root

    @property
    def root(self) -> Path:
        return self._root

    def init(self) -> None:
        self._run(["init", "--quiet", "--initial-branch=main"])
        self._run(["config", "user.email", "pipeline-doctor@example.com"])
        self._run(["config", "user.name", "Pipeline Doctor Demo"])

    def commit_all(self, message: str, author: str = "Data Engineer <data-eng@example.com>") -> str:
        self._run(["add", "--all"])
        self._run(["commit", "--quiet", "--allow-empty", f"--author={author}", "-m", message])
        return self.resolve("HEAD")

    def tag(self, name: str) -> None:
        self._run(["tag", "--force", _validate_ref(name)])

    def resolve(self, ref: str) -> str:
        return self._run(["rev-parse", "--verify", "--quiet", _validate_ref(ref)]).strip()

    def has_ref(self, ref: str) -> bool:
        try:
            self.resolve(ref)
        except GitCommandError:
            return False
        return True

    def list_commits(self, since_ref: str | None, until_ref: str, max_count: int) -> list[CommitInfo]:
        until = _validate_ref(until_ref)
        revision = f"{_validate_ref(since_ref)}..{until}" if since_ref else until
        output = self._run(
            [
                "log",
                f"--max-count={max_count}",
                f"--format={_RECORD_SEPARATOR}%H{_FIELD_SEPARATOR}%an <%ae>{_FIELD_SEPARATOR}%aI{_FIELD_SEPARATOR}%s",
                "--name-only",
                "--relative",
                revision,
            ]
        )
        return [_parse_commit(record) for record in output.split(_RECORD_SEPARATOR) if record.strip()]

    def show_diff(self, sha: str, paths: list[str]) -> str:
        return self._run(["show", "--format=", "--unified=3", _validate_ref(sha), "--", *paths])

    def has_parent(self, sha: str) -> bool:
        return self.has_ref(f"{sha}^")

    def show_file(self, ref: str, path: str) -> str:
        return self._run(["show", f"{_validate_ref(ref)}:./{_validate_path(path)}"])

    def create_branch_commit(self, branch: str, message: str, files: dict[str, str]) -> str:
        """Commits file contents onto a new branch from HEAD without touching the working tree or index."""
        prefix = self._run(["rev-parse", "--show-prefix"]).strip()
        git_dir = Path(self._run(["rev-parse", "--absolute-git-dir"]).strip())
        index_file = git_dir / f"pipeline-doctor-{uuid4().hex}.index"
        environment = {**os.environ, "GIT_INDEX_FILE": str(index_file), **self._fallback_identity()}
        try:
            self._run(["read-tree", "HEAD"], environment)
            for path, content in files.items():
                blob = self._run(["hash-object", "-w", "--stdin"], environment, stdin=content).strip()
                self._run(
                    ["update-index", "--add", "--cacheinfo", f"100644,{blob},{prefix}{_validate_path(path)}"],
                    environment,
                )
            tree = self._run(["write-tree"], environment).strip()
            commit = self._run(["commit-tree", tree, "-p", "HEAD", "-m", message], environment).strip()
            self._run(["update-ref", f"refs/heads/{_validate_ref(branch)}", commit])
        finally:
            index_file.unlink(missing_ok=True)
        return commit

    def _fallback_identity(self) -> dict[str, str]:
        """Supplies a commit identity only when git has none configured, so commit-tree works on bare servers."""
        try:
            has_identity = bool(
                self._run(["config", "user.name"]).strip() and self._run(["config", "user.email"]).strip()
            )
        except GitCommandError:
            has_identity = False
        if has_identity:
            return {}
        return {
            "GIT_AUTHOR_NAME": "Pipeline Doctor",
            "GIT_AUTHOR_EMAIL": "pipeline-doctor@example.com",
            "GIT_COMMITTER_NAME": "Pipeline Doctor",
            "GIT_COMMITTER_EMAIL": "pipeline-doctor@example.com",
        }

    def _run(self, arguments: list[str], environment: dict[str, str] | None = None, stdin: str | None = None) -> str:
        logger.debug(f"run git {arguments[0]} {self._root}")
        completed = subprocess.run(  # noqa: S603 - fixed argument vector, no shell
            ["git", *arguments],  # noqa: S607
            cwd=self._root,
            env=environment,
            input=stdin,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise GitCommandError(f"git {arguments[0]} failed: {completed.stderr.strip()}")
        return completed.stdout


def _validate_ref(ref: str) -> str:
    if not _SAFE_REF.match(ref) or ref.startswith("-"):
        raise GitCommandError(f"unsafe git ref: {ref!r}")
    return ref


def _validate_path(path: str) -> str:
    if path.startswith(("/", "-")) or ".." in Path(path).parts:
        raise GitCommandError(f"unsafe path: {path!r}")
    return path


def _parse_commit(record: str) -> CommitInfo:
    header, _, body = record.strip("\n").partition("\n")
    sha, author, authored_at, message = header.split(_FIELD_SEPARATOR)
    files = [line for line in body.splitlines() if line.strip()]
    return CommitInfo(
        sha=sha, author=author, authored_at=datetime.fromisoformat(authored_at), message=message, files=files
    )
