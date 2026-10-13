import difflib
from pathlib import Path, PurePosixPath

from loguru import logger

from pipeline_doctor.incidents.models import FileEdit

_ALLOWED_TOP_LEVEL = {"models", "tests", "macros", "snapshots", "seeds", "analyses"}
_ALLOWED_SUFFIXES = {".sql", ".yml", ".yaml", ".md", ".csv"}
_MAX_FILE_BYTES = 200_000


class FixApplicationError(ValueError):
    pass


def validate_project_path(path: str) -> PurePosixPath:
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
        raise FixApplicationError(f"{path}: must be a relative path inside the dbt project")
    if candidate.parts[0] not in _ALLOWED_TOP_LEVEL or candidate.suffix not in _ALLOWED_SUFFIXES:
        raise FixApplicationError(
            f"{path}: fixes may only touch {sorted(_ALLOWED_SUFFIXES)} files under {sorted(_ALLOWED_TOP_LEVEL)}"
        )
    return candidate


def resolve_project_file(project_dir: Path, path: str) -> Path:
    """Maps a validated relative path to a file inside the project, refusing symlinks anywhere on the way."""
    root = project_dir.resolve()
    target = root / validate_project_path(path)
    current = root
    for part in target.relative_to(root).parts:
        current = current / part
        if current.is_symlink():
            raise FixApplicationError(f"{path}: symlinks are not allowed in fix paths")
    if not target.resolve().is_relative_to(root):
        raise FixApplicationError(f"{path}: resolves outside the dbt project")
    return target


def read_project_file(project_dir: Path, path: str) -> str:
    target = resolve_project_file(project_dir, path)
    logger.debug(f"read project file {target}")
    return target.read_text() if target.exists() else ""


def apply_edits(project_dir: Path, edits: list[FileEdit]) -> dict[str, str]:
    """Applies snippet edits in memory and returns the new content of every touched file."""
    contents: dict[str, str] = {}
    for edit in edits:
        relative = str(validate_project_path(edit.path))
        current = contents[relative] if relative in contents else read_project_file(project_dir, relative)
        contents[relative] = _apply_one(relative, current, edit)
    return contents


def render_diff(project_dir: Path, contents: dict[str, str]) -> str:
    chunks = []
    for path, updated in sorted(contents.items()):
        original = read_project_file(project_dir, path)
        chunks.extend(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                updated.splitlines(keepends=True),
                fromfile=f"a/{path}",
                tofile=f"b/{path}",
            )
        )
    return "".join(chunks)


def build_revert_edits(path: str, committed: str, before: str, context_lines: int = 2) -> list[FileEdit]:
    """Edits that undo only one commit's hunks, so later changes to the same file survive the revert."""
    committed_lines = committed.splitlines(keepends=True)
    before_lines = before.splitlines(keepends=True)
    matcher = difflib.SequenceMatcher(a=committed_lines, b=before_lines, autojunk=False)
    edits = []
    for group in matcher.get_grouped_opcodes(context_lines):
        start, end = group[0][1], group[-1][2]
        replacement = []
        for tag, a_start, a_end, b_start, b_end in group:
            replacement.extend(committed_lines[a_start:a_end] if tag == "equal" else before_lines[b_start:b_end])
        edits.append(FileEdit(path=path, original="".join(committed_lines[start:end]), updated="".join(replacement)))
    return edits


def write_project_files(project_dir: Path, contents: dict[str, str]) -> None:
    for path, content in contents.items():
        target = resolve_project_file(project_dir, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"write project file {target}")
        target.write_text(content)


def _apply_one(path: str, current: str, edit: FileEdit) -> str:
    if not edit.original:
        if current:
            raise FixApplicationError(f"{path}: an empty `original` is only allowed when creating a new file")
        updated = edit.updated
    else:
        occurrences = current.count(edit.original)
        if occurrences != 1:
            raise FixApplicationError(
                f"{path}: the `original` snippet must occur exactly once, found {occurrences} occurrences"
            )
        updated = current.replace(edit.original, edit.updated)
    if len(updated.encode()) > _MAX_FILE_BYTES:
        raise FixApplicationError(f"{path}: resulting file exceeds {_MAX_FILE_BYTES} bytes")
    return updated
