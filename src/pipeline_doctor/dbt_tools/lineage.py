import re

import sqlglot
from loguru import logger
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node as LineageNode
from sqlglot.lineage import lineage

from pipeline_doctor.dbt_tools.artifacts import Manifest, ManifestNode, ResourceType

_PACKAGE_PATH_PREFIX = re.compile(r"^[\w-]+://")
_MAX_TRACE_DEPTH = 6

type CatalogSchema = dict[str, dict[str, dict[str, dict[str, str]]]]
"""Column types nested as database -> schema -> table -> column -> type, the shape sqlglot expects."""


class DbtLineage:
    """Graph queries over a dbt manifest: ancestors, descendants, exposures, and column lineage."""

    def __init__(self, manifest: Manifest, catalog: CatalogSchema | None = None) -> None:
        self._manifest = manifest
        self._all = {**manifest.nodes, **manifest.sources, **manifest.exposures}
        self._catalog = catalog or _catalog_from_manifest(manifest)

    def get(self, unique_id: str) -> ManifestNode | None:
        return self._all.get(unique_id)

    def find(self, short_name: str) -> ManifestNode | None:
        wanted = short_name.lower().strip()
        for node in self._all.values():
            if node.resource_type in {ResourceType.MODEL, ResourceType.SOURCE, ResourceType.SEED} and (
                node.short_name.lower() == wanted or node.unique_id.lower() == wanted
            ):
                return node
        return None

    def get_tested_node(self, test: ManifestNode) -> ManifestNode | None:
        if test.attached_node:
            return self.get(test.attached_node)
        parents = [self.get(node_id) for node_id in test.depends_on.nodes]
        return next((node for node in parents if node is not None), None)

    def list_upstream(self, unique_id: str) -> list[ManifestNode]:
        return self._walk(unique_id, self._manifest.parent_map)

    def list_downstream(self, unique_id: str) -> list[ManifestNode]:
        return self._walk(unique_id, self._manifest.child_map)

    def list_impacted_exposures(self, unique_id: str) -> list[ManifestNode]:
        return [node for node in self.list_downstream(unique_id) if node.resource_type is ResourceType.EXPOSURE]

    def list_staging_models(self, source_id: str) -> list[ManifestNode]:
        children = [self.get(child) for child in self._manifest.child_map.get(source_id, [])]
        return [node for node in children if node is not None and node.resource_type is ResourceType.MODEL]

    def list_lineage_files(self, unique_id: str) -> list[str]:
        node = self.get(unique_id)
        if node is None:
            return []
        related = [node, *self.list_upstream(unique_id)]
        if node.resource_type is ResourceType.TEST:
            tested = self.get_tested_node(node)
            related.extend([tested, *self.list_upstream(tested.unique_id)] if tested else [])
        owned = [item for item in related if self._is_project_owned(item)]
        paths = {item.original_file_path for item in owned if item.original_file_path}
        paths |= {_strip_package(item.patch_path) for item in owned if item.patch_path}
        return sorted(paths)

    def _is_project_owned(self, node: ManifestNode) -> bool:
        root = self._manifest.metadata.project_name
        return not root or not node.package_name or node.package_name == root

    def trace_column_to_source(self, model: ManifestNode, column: str) -> tuple[ManifestNode, str] | None:
        """Follows a model column back through upstream models to the source column it is read from."""
        current, current_column = model, column
        for _ in range(_MAX_TRACE_DEPTH):
            if current.resource_type is ResourceType.SOURCE:
                return current, current_column
            parent_hit = self._trace_one_hop(current, current_column)
            if parent_hit is None:
                return None
            current, current_column = parent_hit
        return None

    def _trace_one_hop(self, model: ManifestNode, column: str) -> tuple[ManifestNode, str] | None:
        parents = [parent for node_id in model.depends_on.nodes if (parent := self.get(node_id)) is not None]
        if not parents:
            return None
        parent_column = self._find_parent_column(model, column, parents)
        if parent_column is None:
            return None
        table_name, source_column = parent_column
        for parent in parents:
            identifier = (parent.identifier or parent.name).lower()
            if table_name in {identifier, parent.name.lower()} or len(parents) == 1:
                return parent, source_column
        return None

    def _find_parent_column(
        self, model: ManifestNode, column: str, parents: list[ManifestNode]
    ) -> tuple[str, str] | None:
        if model.compiled_code:
            traced = _trace_with_sqlglot(model.compiled_code, column, self._catalog)
            if traced:
                return traced
        return _trace_with_regex(model.raw_code, column, parents) if len(parents) == 1 else None

    def _walk(self, unique_id: str, edges: dict[str, list[str]]) -> list[ManifestNode]:
        seen: list[str] = []
        frontier = list(edges.get(unique_id, []))
        while frontier:
            current = frontier.pop(0)
            if current in seen:
                continue
            seen.append(current)
            frontier.extend(edges.get(current, []))
        nodes = [self.get(node_id) for node_id in seen]
        return [node for node in nodes if node is not None and node.resource_type is not ResourceType.TEST]


def _strip_package(path: str) -> str:
    return _PACKAGE_PATH_PREFIX.sub("", path)


def _catalog_from_manifest(manifest: Manifest) -> CatalogSchema:
    catalog: CatalogSchema = {}
    for node in [*manifest.sources.values(), *manifest.nodes.values()]:
        if not node.relation_name or not node.columns:
            continue
        parts = [part.strip('"') for part in node.relation_name.split(".")]
        if len(parts) != 3:
            continue
        database, schema, table = parts
        columns = {name: info.data_type or "VARCHAR" for name, info in node.columns.items()}
        catalog.setdefault(database, {}).setdefault(schema, {})[table] = columns
    return catalog


def _trace_with_sqlglot(sql: str, column: str, catalog: CatalogSchema) -> tuple[str, str] | None:
    try:
        root = lineage(column, sql, schema=catalog, dialect="duckdb")
    except (SqlglotError, KeyError, ValueError) as error:
        logger.debug(f"sqlglot lineage fallback for column {column}: {error}")
        return None
    leaf = _first_leaf(root)
    if leaf is None or not isinstance(leaf.expression, sqlglot.exp.Table):
        return None
    leaf_column = leaf.name.split(".")[-1].strip('"').lower()
    if leaf_column == "*":
        return None
    return leaf.expression.name.lower(), leaf_column


def _first_leaf(node: LineageNode) -> LineageNode | None:
    for candidate in node.walk():
        if not candidate.downstream:
            return candidate
    return None


def _trace_with_regex(raw_code: str, column: str, parents: list[ManifestNode]) -> tuple[str, str] | None:
    name = re.escape(column)
    patterns = [
        rf"cast\(\s*(?:\w+\.)?(\w+)\s+as\s+[\w(), ]+\)\s+as\s+{name}\b",
        rf"(?:\w+\.)?(\w+)\s+as\s+{name}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_code, flags=re.IGNORECASE)
        if match:
            return parents[0].name.lower(), match.group(1).lower()
    if re.search(rf"\b{name}\b", raw_code, flags=re.IGNORECASE) or "select *" in raw_code.lower():
        return parents[0].name.lower(), column.lower()
    return None
