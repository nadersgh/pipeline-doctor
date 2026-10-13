from pathlib import Path

import pytest

from pipeline_doctor.agent.fixes import (
    FixApplicationError,
    apply_edits,
    build_revert_edits,
    render_diff,
    write_project_files,
)
from pipeline_doctor.incidents.models import FileEdit


@pytest.fixture
def project(tmp_path: Path) -> Path:
    model = tmp_path / "models" / "stg_orders.sql"
    model.parent.mkdir(parents=True)
    model.write_text("with source as (\n    select * from {{ source('raw', 'orders') }}\n)\nselect * from source\n")
    return tmp_path


def test_apply_edits_replaces_a_unique_snippet(project: Path) -> None:
    # Setup
    edit = FileEdit(
        path="models/stg_orders.sql", original="select * from {{", updated="select * rename (a as b) from {{"
    )

    # Act
    contents = apply_edits(project, [edit])

    # Assert
    assert "rename (a as b)" in contents["models/stg_orders.sql"]
    assert "rename" not in (project / "models/stg_orders.sql").read_text()


def test_apply_edits_rejects_ambiguous_snippet(project: Path) -> None:
    with pytest.raises(FixApplicationError, match="exactly once"):
        apply_edits(project, [FileEdit(path="models/stg_orders.sql", original="select *", updated="select 1")])


@pytest.mark.parametrize(
    "path", ["../outside.sql", "/etc/passwd", "dbt_project.yml", "models/script.py", "profiles.yml"]
)
def test_apply_edits_rejects_paths_outside_the_allowlist(project: Path, path: str) -> None:
    with pytest.raises(FixApplicationError):
        apply_edits(project, [FileEdit(path=path, original="", updated="x")])


def test_apply_edits_creates_new_test_file(project: Path) -> None:
    # Act
    contents = apply_edits(project, [FileEdit(path="tests/assert_x.sql", original="", updated="select 1 where false")])

    # Assert
    assert contents == {"tests/assert_x.sql": "select 1 where false"}


def test_render_diff_and_write_project_files_round_trip(project: Path) -> None:
    # Setup
    contents = apply_edits(
        project,
        [FileEdit(path="models/stg_orders.sql", original="select * from source", updated="select id from source")],
    )

    # Act
    diff = render_diff(project, contents)
    write_project_files(project, contents)

    # Assert
    assert "-select * from source" in diff
    assert "+select id from source" in diff
    assert (project / "models/stg_orders.sql").read_text().endswith("select id from source\n")


def test_apply_edits_refuses_symlinks_that_escape_the_project(project: Path, tmp_path: Path) -> None:
    # Setup
    secret = tmp_path.parent / "secret.sql"
    secret.write_text("select 'secret'")
    (project / "models" / "leak.sql").symlink_to(secret)

    # Act / Assert
    with pytest.raises(FixApplicationError, match="symlink"):
        apply_edits(project, [FileEdit(path="models/leak.sql", original="secret", updated="x")])


def test_build_revert_edits_undoes_one_commit_and_keeps_later_changes(project: Path) -> None:
    # Setup
    before = "select\n    id,\n    amount / 100.0 as amount,\n    status\nfrom source\n"
    committed = "select\n    id,\n    amount as amount,\n    status\nfrom source\n"
    current = "-- owner: finance\nselect\n    id,\n    amount as amount,\n    status\nfrom source\n"
    (project / "models" / "stg_payments.sql").write_text(current)

    # Act
    edits = build_revert_edits("models/stg_payments.sql", committed, before)
    contents = apply_edits(project, edits)

    # Assert
    assert contents["models/stg_payments.sql"] == "-- owner: finance\n" + before
