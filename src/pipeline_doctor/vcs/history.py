from datetime import datetime

from pydantic import BaseModel, Field

from pipeline_doctor.vcs.repository import GitRepository

_MAX_DIFF_CHARS = 6000
_SQL_COMMENT_PREFIXES = ("--", "/*", "*", "{#")


class CommitChange(BaseModel):
    sha: str
    author: str
    authored_at: datetime
    message: str
    files: list[str]
    lineage_files: list[str] = Field(default_factory=list)
    changes_lineage_sql: bool = False
    diff: str = ""

    @property
    def touches_lineage(self) -> bool:
        return bool(self.lineage_files)

    def describe(self) -> str:
        scope = (
            f"touches lineage files {self.lineage_files}"
            + (" with SQL logic changes" if self.changes_lineage_sql else " (no SQL logic change)")
            if self.touches_lineage
            else "outside the failing lineage"
        )
        return f"{self.sha[:8]} by {self.author} at {self.authored_at:%Y-%m-%d %H:%M}: {self.message!r}; {scope}"


class GitHistory:
    """Read-only view of recent commits relevant to a failing dbt node."""

    def __init__(self, repository: GitRepository, since_ref: str, max_commits: int) -> None:
        self._repository = repository
        self._since_ref = since_ref
        self._max_commits = max_commits

    def list_recent_changes(self, lineage_files: list[str], deployed_ref: str = "HEAD") -> list[CommitChange]:
        """Commits after the last green run up to the deployed commit, with diffs limited to lineage files."""
        since = self._since_ref if self._repository.has_ref(self._since_ref) else None
        commits = self._repository.list_commits(since, deployed_ref, self._max_commits)
        changes = []
        for commit in commits:
            if not self._repository.has_parent(commit.sha):
                continue
            relevant = sorted(set(commit.files) & set(lineage_files))
            diff = self._repository.show_diff(commit.sha, relevant)[:_MAX_DIFF_CHARS] if relevant else ""
            changes.append(
                CommitChange(
                    sha=commit.sha,
                    author=commit.author,
                    authored_at=commit.authored_at,
                    message=commit.message,
                    files=commit.files,
                    lineage_files=relevant,
                    changes_lineage_sql=has_sql_logic_change(diff),
                    diff=diff,
                )
            )
        return changes

    def read_file_before(self, sha: str, path: str) -> str:
        return self._repository.show_file(f"{sha}^", path)

    def read_file_at(self, sha: str, path: str) -> str:
        return self._repository.show_file(sha, path)


def has_sql_logic_change(diff: str) -> bool:
    """True when a unified diff changes a non-comment, non-blank line of a .sql file."""
    in_sql_file = False
    for line in diff.splitlines():
        if line.startswith("diff --git"):
            in_sql_file = line.endswith(".sql")
            continue
        if not in_sql_file or line.startswith(("+++", "---")) or not line.startswith(("+", "-")):
            continue
        content = line[1:].strip()
        if content and not content.startswith(_SQL_COMMENT_PREFIXES):
            return True
    return False
