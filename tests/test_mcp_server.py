import asyncio
from typing import Any

import pytest

from pipeline_doctor.mcp_server import create_mcp_server
from pipeline_doctor.settings import Settings


def test_every_mcp_tool_declares_all_four_annotation_hints() -> None:
    # Setup
    server = create_mcp_server(Settings())

    # Act
    tools = asyncio.run(server.list_tools())

    # Assert
    assert len(tools) == 9
    for tool in tools:
        hints = (
            tool.annotations.read_only_hint,
            tool.annotations.destructive_hint,
            tool.annotations.idempotent_hint,
            tool.annotations.open_world_hint,
        )
        assert all(isinstance(hint, bool) for hint in hints), tool.name


def test_only_the_diagnose_tool_is_not_read_only() -> None:
    # Setup
    server = create_mcp_server(Settings())

    # Act
    tools = asyncio.run(server.list_tools())

    # Assert
    writers = {tool.name for tool in tools if not tool.annotations.read_only_hint}
    assert writers == {"diagnose_airflow_failure"}


@pytest.mark.integration
@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("list_failed_nodes", {}),
        ("get_recent_commits", {"node": "fct_orders"}),
        ("run_diagnostic_check", {"kind": "row_count", "relation": "raw.orders"}),
        ("search_past_incidents", {"text": "renamed column on raw.orders"}),
    ],
)
def test_mcp_tool_answers_on_a_green_demo_project(
    demo_settings: Settings, tool: str, arguments: dict[str, Any]
) -> None:
    # Setup
    server = create_mcp_server(demo_settings)

    # Act
    result = asyncio.run(server.call_tool(tool, arguments))

    # Assert
    assert not result.is_error
    assert result.structured_content is not None
