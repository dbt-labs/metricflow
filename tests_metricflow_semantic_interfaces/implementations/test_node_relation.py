from __future__ import annotations

from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation


def test_node_relation_compiled_sql_defaults_to_none() -> None:
    """Test that compiled_sql is None by default for non-ephemeral models."""
    node_relation = PydanticNodeRelation(schema_name="my_schema", alias="my_table")
    assert node_relation.compiled_sql is None
    assert node_relation.relation_name == "my_schema.my_table"


def test_node_relation_with_compiled_sql() -> None:
    """Test that compiled_sql is preserved when set (ephemeral model)."""
    compiled_sql = "SELECT id, name FROM raw.source_table WHERE active = true"
    node_relation = PydanticNodeRelation(
        schema_name="my_schema",
        alias="my_table",
        compiled_sql=compiled_sql,
    )
    assert node_relation.compiled_sql == compiled_sql
    assert node_relation.relation_name == "my_schema.my_table"


def test_node_relation_from_string_has_no_compiled_sql() -> None:
    """Test that from_string produces a node relation without compiled_sql."""
    node_relation = PydanticNodeRelation.from_string("my_schema.my_table")
    assert node_relation.compiled_sql is None


def test_node_relation_with_database_and_compiled_sql() -> None:
    """Test compiled_sql with a fully qualified relation (database.schema.table)."""
    compiled_sql = "SELECT 1"
    node_relation = PydanticNodeRelation(
        database="my_db",
        schema_name="my_schema",
        alias="my_table",
        compiled_sql=compiled_sql,
    )
    assert node_relation.compiled_sql == compiled_sql
    assert node_relation.relation_name == "my_db.my_schema.my_table"
