from __future__ import annotations

from typing import Any, Dict

from dbt_semantic_interfaces.parsing.schema_validator import SchemaValidator
from dbt_semantic_interfaces.parsing.schemas import (
    aggregation_type_params_schema,
    derived_group_by_element_schema,
    dimension_schema,
    dimension_type_params_schema,
    entity_schema,
    measure_schema,
    metric_input_measure_schema,
    metric_input_schema,
    metric_schema,
    metric_type_params_schema,
    node_relation_schema,
    non_additive_dimension_schema,
    semantic_model_schema,
    validity_params_schema,
)
from jsonschema import RefResolver


def add_transform_metadata_fields_to_spec(spec: Dict[str, Any]) -> None:  # type: ignore[misc]
    """Adds transform metadata fields a spec."""
    properties = spec["properties"]
    transform_metadata_fields = {
        "description": {"type": "string"},
        "owners": {
            "type": "array",
            "items": {"type": "string"},
        },
        "display_name": {"type": "string"},
        "tier": {"type": ["string", "integer"]},
    }

    for k, v in transform_metadata_fields.items():
        if k in properties:
            raise RuntimeError(f"Spec with id: {spec['$id']} contains transform metadata field: {k}")

        properties[k] = v


def add_locked_metadata_to_spec(spec: Dict[str, Any]) -> None:  # type: ignore[misc]
    """Adds locked metadata field to a spec."""
    properties = spec["properties"]
    transform_metadata_fields = {
        "locked_metadata": {"$ref": "locked_metadata"},
    }

    for k, v in transform_metadata_fields.items():
        if k in properties:
            raise RuntimeError(f"Spec with id: {spec['$id']} contains transform metadata field: {k}")

        properties[k] = v


# Sub-object schemas
locked_metadata_schema = {
    "$id": "locked_metadata",
    "type": "object",
    "properties": {
        "value_format": {"type": "string"},
        "description": {"type": "string"},
        "display_name": {"type": "string"},
        "tier": {"type": ["string", "integer"]},
        "increase_is_good": {"type": "boolean"},
        "tags": {
            "type": "array",
            "items": {"type": "string"},
        },
        "private": {"type": "boolean"},
        "unit": {"type": "string"},
    },
    "additionalProperties": False,
}


# Add transform metadata fields to top level objects (metric, semantic model, derived entity)
add_transform_metadata_fields_to_spec(dimension_schema)
add_transform_metadata_fields_to_spec(measure_schema)
add_transform_metadata_fields_to_spec(entity_schema)

add_transform_metadata_fields_to_spec(metric_schema)
add_locked_metadata_to_spec(metric_schema)

add_transform_metadata_fields_to_spec(semantic_model_schema)
add_transform_metadata_fields_to_spec(derived_group_by_element_schema)


schema_store = {
    # Top level schemas
    metric_schema["$id"]: metric_schema,
    semantic_model_schema["$id"]: semantic_model_schema,
    derived_group_by_element_schema["$id"]: derived_group_by_element_schema,
    # Sub-object schemas
    metric_input_measure_schema["$id"]: metric_input_measure_schema,
    metric_type_params_schema["$id"]: metric_type_params_schema,
    locked_metadata_schema["$id"]: locked_metadata_schema,
    entity_schema["$id"]: entity_schema,
    measure_schema["$id"]: measure_schema,
    dimension_schema["$id"]: dimension_schema,
    validity_params_schema["$id"]: validity_params_schema,
    dimension_type_params_schema["$id"]: dimension_type_params_schema,
    aggregation_type_params_schema["$id"]: aggregation_type_params_schema,
    non_additive_dimension_schema["$id"]: non_additive_dimension_schema,
    metric_input_schema["$id"]: metric_input_schema,
    node_relation_schema["$id"]: node_relation_schema,
}

resolver = RefResolver.from_schema(schema=metric_schema, store=schema_store)
semantic_model_validator = SchemaValidator(semantic_model_schema, resolver=resolver)
derived_group_by_element_validator = SchemaValidator(derived_group_by_element_schema, resolver=resolver)
metric_validator = SchemaValidator(metric_schema, resolver=resolver)
