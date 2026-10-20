from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from pipeline_doctor.vcs.history import GitHistory, has_sql_logic_change
from pipeline_doctor.vcs.repository import GitCommandError, GitRepository


@pytest.fixture
def repository(tmp_path: Path) -> GitRepository:
    repo = GitRepository(tmp_path)
    repo.init()
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "stg_orders.sql").write_text("select id from raw.orders\n")
    (tmp_path / "models" / "dim.sql").write_text("select 1\n")
    repo.commit_all("initial")
    repo.tag("last-green")
    return repo


@pytest.mark.parametrize(
    ("diff", "expected"),
    [
        ("diff --git a/m.sql b/m.sql\n--- a/m.sql\n+++ b/m.sql\n-select a\n+select b\n", True),
        ("diff --git a/m.sql b/m.sql\n--- a/m.sql\n+++ b/m.sql\n+-- owner: data team\n", False),
        ("diff --git a/m.yml b/m.yml\n--- a/m.yml\n+++ b/m.yml\n-description: a\n+description: b\n", False),
        ("diff --git a/m.sql b/m.sql\n--- a/m.sql\n+++ b/m.sql\n+\n", False),
    ],
)
def test_has_sql_logic_change_ignores_comments_docs_and_blank_lines(diff: str, expected: bool) -> None:
    assert has_sql_logic_change(diff) is expected


def test_list_recent_changes_flags_commits_touching_lineage(repository: GitRepository) -> None:
    # Setup
    (repository.root / "models" / "stg_orders.sql").write_text("select id, status from raw.orders\n")
    repository.commit_all("feat: add status")
    (repository.root / "models" / "dim.sql").write_text("select 2\n")
    repository.commit_all("chore: unrelated")
    history = GitHistory(repository, since_ref="last-green", max_commits=10)

    # Act
    changes = history.list_recent_changes(["models/stg_orders.sql"])

    # Assert
    assert [change.message for change in changes] == ["chore: unrelated", "feat: add status"]
    assert [change.changes_lineage_sql for change in changes] == [False, True]
    assert "+select id, status from raw.orders" in changes[1].diff


def test_list_recent_changes_without_green_tag_skips_root_commit(tmp_path: Path) -> None:
    # Setup
    repo = GitRepository(tmp_path)
    repo.init()
    (tmp_path / "a.sql").write_text("select 1\n")
    repo.commit_all("initial")

    # Act
    changes = GitHistory(repo, since_ref="missing-tag", max_commits=5).list_recent_changes(["a.sql"])

    # Assert
    assert changes == []


def test_read_file_before_returns_parent_version(repository: GitRepository) -> None:
    # Setup
    (repository.root / "models" / "stg_orders.sql").write_text("select broken\n")
    sha = repository.commit_all("break it")

    # Act
    before = GitHistory(repository, "last-green", 5).read_file_before(sha, "models/stg_orders.sql")

    # Assert
    assert before == "select id from raw.orders\n"


@pytest.mark.parametrize("ref", ["--upload-pack=evil", "HEAD; rm -rf /", "a b"])
def test_git_repository_rejects_unsafe_refs(repository: GitRepository, ref: str) -> None:
    with pytest.raises(GitCommandError):
        repository.resolve(ref)


def test_create_branch_commit_leaves_working_tree_untouched(repository: GitRepository) -> None:
    # Act
    repository.create_branch_commit("fix/x", "fix", {"models/stg_orders.sql": "select fixed\n"})

    # Assert
    assert (repository.root / "models" / "stg_orders.sql").read_text() == "select id from raw.orders\n"
    assert repository.show_file("fix/x", "models/stg_orders.sql") == "select fixed\n"


def test_list_recent_changes_stops_at_the_deployed_commit(repository: GitRepository) -> None:
    # Setup
    (repository.root / "models" / "stg_orders.sql").write_text("select id, status from raw.orders\n")
    deployed = repository.commit_all("feat: deployed change")
    (repository.root / "models" / "stg_orders.sql").write_text("select id, status, total from raw.orders\n")
    repository.commit_all("feat: merged after the run")

    # Act
    changes = GitHistory(repository, "last-green", 10).list_recent_changes(["models/stg_orders.sql"], deployed)

    # Assert
    assert [change.message for change in changes] == ["feat: deployed change"]


def test_create_branch_commit_runs_concurrently_without_corrupting_commits(repository: GitRepository) -> None:
    # Setup
    branches = {f"fix/{number}": f"select {number}\n" for number in range(8)}

    # Act
    with ThreadPoolExecutor(max_workers=len(branches)) as executor:
        list(
            executor.map(
                lambda item: repository.create_branch_commit(item[0], "fix", {"models/stg_orders.sql": item[1]}),
                branches.items(),
            )
        )

    # Assert
    assert {branch: repository.show_file(branch, "models/stg_orders.sql") for branch in branches} == branches
    assert list(repository.root.joinpath(".git").glob("pipeline-doctor-*.index")) == []
