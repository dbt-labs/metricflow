from __future__ import annotations

from typing import List, Tuple

from referencing import Registry, Resource
from referencing.jsonschema import DRAFT7

from metricflow_semantic_interfaces.parsing.schema_validator import SchemaValidator

TRANSFORM_OBJECT_NAME_PATTERN = "(?!.*__).*^[a-z][a-z0-9_]*[a-z0-9]$"


# Enums
metric_types_enum_values = ["SIMPLE", "RATIO", "CUMULATIVE", "DERIVED", "CONVERSION"]
metric_types_enum_values += [x.lower() for x in metric_types_enum_values]

calculation_types_enum_values = ["CONVERSIONS", "CONVERSION_RATE"]
calculation_types_enum_values += [x.lower() for x in calculation_types_enum_values]

entity_type_enum_values = ["PRIMARY", "UNIQUE", "FOREIGN", "NATURAL"]
entity_type_enum_values += [x.lower() for x in entity_type_enum_values]

aggregation_type_values = [
    "SUM",
    "MIN",
    "MAX",
    "AVERAGE",
    "COUNT_DISTINCT",
    "SUM_BOOLEAN",
    "COUNT",
    "PERCENTILE",
    "MEDIAN",
]
aggregation_type_values += [x.lower() for x in aggregation_type_values]

window_aggregation_type_values = ["MIN", "MAX"]
window_aggregation_type_values += [x.lower() for x in window_aggregation_type_values]

time_granularity_values = [
    "NANOSECOND",
    "MICROSECOND",
    "MILLISECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
]
time_granularity_values += [x.lower() for x in time_granularity_values]

dimension_type_values = ["CATEGORICAL", "TIME"]
dimension_type_values += [x.lower() for x in dimension_type_values]

time_dimension_type_values = ["TIME", "time"]

export_destination_type_values = ["TABLE", "VIEW"]
export_destination_type_values += [x.lower() for x in export_destination_type_values]

period_agg_values = ["FIRST", "LAST", "AVERAGE"]
period_agg_values += [x.lower() for x in period_agg_values]


filter_schema = {
    "$id": "filter_schema",
    "oneOf": [
        {"type": "string"},
        {
            "type": "array",
            "items": {"type": "string"},
        },
    ],
}

metric_input_measure_schema = {
    "$id": "metric_input_measure_schema",
    "oneOf": [
        {"type": "string"},
        {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "filter": {"$ref": "filter_schema"},
                "alias": {"type": "string"},
                "join_to_timespine": {"type": "boolean"},
                "fill_nulls_with": {"type": "integer"},
            },
            "additionalProperties": False,
        },
    ],
}

metric_input_schema = {
    "$id": "metric_input_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "filter": {"$ref": "filter_schema"},
        "alias": {"type": "string"},
        "offset_window": {"type": "string"},
        "offset_to_grain": {"type": "string"},
    },
    "additionalProperties": False,
}

conversion_type_params_schema = {
    "$id": "conversion_type_params_schema",
    "type": "object",
    "properties": {
        "base_measure": {"$ref": "metric_input_measure_schema"},
        "conversion_measure": {"$ref": "metric_input_measure_schema"},
        "calculation": {"enum": calculation_types_enum_values},
        "entity": {"type": "string"},
        "window": {"type": "string"},
        "constant_properties": {"type": "array", "items": {"$ref": "constant_property_input_schema"}},
    },
    "additionalProperties": False,
    "required": ["base_measure", "conversion_measure", "entity"],
}

cumulative_type_params_schema = {
    "$id": "cumulative_type_params_schema",
    "type": "object",
    "properties": {
        "window": {"type": "string"},
        "grain_to_date": {"type": "string"},
        "period_agg": {"enum": period_agg_values},
    },
    "additionalProperties": False,
    "required": [],
}

constant_property_input_schema = {
    "$id": "constant_property_input_schema",
    "type": "object",
    "properties": {
        "base_property": {"type": "string"},
        "conversion_property": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["base_property", "conversion_property"],
}

metric_type_params_schema = {
    "$id": "metric_type_params",
    "type": "object",
    "properties": {
        "numerator": {"$ref": "metric_input_measure_schema"},
        "denominator": {"$ref": "metric_input_measure_schema"},
        "measure": {"$ref": "metric_input_measure_schema"},
        "expr": {"type": ["string", "boolean"]},
        "window": {"type": "string"},
        "grain_to_date": {"type": "string"},
        "metrics": {
            "type": "array",
            "items": {"$ref": "metric_input_schema"},
        },
        "conversion_type_params": {"$ref": "conversion_type_params_schema"},
        "cumulative_type_params": {"$ref": "cumulative_type_params_schema"},
    },
    "additionalProperties": False,
}


entity_config_schema = {
    "$id": "entity_config_schema",
    "type": "object",
    "properties": {
        "meta": {"type": "object", "propertyNames": {"type": "string"}},
    },
    "additionalProperties": False,
}

entity_schema = {
    "$id": "entity_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "type": {"enum": entity_type_enum_values},
        "role": {"type": "string"},
        "expr": {"type": ["string", "boolean"]},
        "entity": {"type": "string"},
        "label": {"type": "string"},
        "config": {"$ref": "entity_config_schema"},
    },
    "additionalProperties": False,
    "required": ["name", "type"],
}

validity_params_schema = {
    "$id": "validity_params_schema",
    "type": "object",
    "properties": {
        "is_start": {"type": "boolean"},
        "is_end": {"type": "boolean"},
    },
    "additionalProperties": False,
}

dimension_type_params_schema = {
    "$id": "dimension_type_params_schema",
    "type": "object",
    "properties": {
        "time_granularity": {"enum": time_granularity_values},
        "validity_params": {"$ref": "validity_params_schema"},
    },
    "additionalProperties": False,
    "required": ["time_granularity"],
}

non_additive_dimension_schema = {
    "$id": "non_additive_dimension_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "window_choice": {"enum": window_aggregation_type_values},
        "window_groupings": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "additionalProperties": False,
    "required": ["name"],
}

aggregation_type_params_schema = {
    "$id": "aggregation_type_params_schema",
    "type": "object",
    "properties": {
        "percentile": {"type": "number"},
        "use_discrete_percentile": {"type": "boolean"},
        "use_approximate_percentile": {"type": "boolean"},
    },
    "additionalProperties": False,
}

measure_config_schema = {
    "$id": "measure_config_schema",
    "type": "object",
    "properties": {
        "meta": {"type": "object", "propertyNames": {"type": "string"}},
    },
    "additionalProperties": False,
}

measure_schema = {
    "$id": "measure_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "agg": {"enum": aggregation_type_values},
        "agg_time_dimension": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "expr": {"type": ["string", "integer", "boolean"]},
        "agg_params": {"$ref": "aggregation_type_params_schema"},
        "create_metric": {"type": "boolean"},
        "create_metric_display_name": {"type": "string"},
        "non_additive_dimension": {
            "$ref": "non_additive_dimension_schema",
        },
        "description": {"type": "string"},
        "label": {"type": "string"},
        "config": {"$ref": "measure_config_schema"},
    },
    "additionalProperties": False,
    "required": ["name", "agg"],
}

dimension_config_schema = {
    "$id": "dimension_config_schema",
    "type": "object",
    "properties": {
        "meta": {"type": "object", "propertyNames": {"type": "string"}},
    },
    "additionalProperties": False,
}

dimension_schema = {
    "$id": "dimension_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "description": {"type": "string"},
        "type": {"enum": dimension_type_values},
        "is_partition": {"type": "boolean"},
        "expr": {"type": ["string", "boolean"]},
        "type_params": {"$ref": "dimension_type_params_schema"},
        "label": {"type": "string"},
        "config": {"$ref": "dimension_config_schema"},
    },
    # dimension must have type_params if its a time dimension
    "anyOf": [{"not": {"$ref": "#/definitions/is-time-dimension"}}, {"required": ["type_params"]}],
    "definitions": {
        "is-time-dimension": {
            "properties": {"type": {"enum": time_dimension_type_values}},
            "required": ["type"],
        },
    },
    "additionalProperties": False,
    "required": ["name", "type"],
}

metric_config_schema = {
    "$id": "metric_config_schema",
    "type": "object",
    "properties": {
        "meta": {"type": "object", "propertyNames": {"type": "string"}},
    },
    "additionalProperties": False,
}

# Top level object schemas
metric_schema = {
    "$id": "metric_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "type": {"enum": metric_types_enum_values},
        "type_params": {"$ref": "metric_type_params"},
        "filter": {"$ref": "filter_schema"},
        "description": {"type": "string"},
        "label": {"type": "string"},
        "config": {"$ref": "metric_config_schema"},
        "time_granularity": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["name", "type", "type_params"],
}

node_relation_schema = {
    "$id": "node_relation_schema",
    "type": "object",
    "properties": {
        "alias": {"type": "string"},
        "schema_name": {"type": "string"},
        "database": {"type": "string"},
        "relation_name": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["alias", "schema_name"],
}


semantic_model_defaults_schema = {
    "$id": "semantic_model_defaults_schema",
    "type": "object",
    "properties": {
        "agg_time_dimension": {"type": "string"},
    },
    "additionalProperties": False,
    "required": [],
}


time_spine_table_configuration_schema = {
    "$id": "time_spine_table_configuration_schema",
    "type": "object",
    "properties": {
        "location": {"type": "string"},
        "column_name": {"type": "string"},
        "grain": {"enum": time_granularity_values},
    },
    "additionalProperties": False,
    "required": ["location", "column_name", "grain"],
}

time_spine_primary_column_schema = {
    "$id": "time_spine_primary_column_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "time_granularity": {"enum": time_granularity_values},
    },
    "additionalProperties": False,
    "required": ["name", "time_granularity"],
}

custom_granularity_column_schema = {
    "$id": "custom_granularity_column_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "column_name": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["name"],
}

time_spine_schema = {
    "$id": "time_spine_schema",
    "type": "object",
    "properties": {
        "node_relation": {"$ref": "node_relation_schema"},
        "primary_column": {"$ref": "time_spine_primary_column_schema"},
        "custom_granularities": {
            "type": "array",
            "items": {"$ref": "custom_granularity_column_schema"},
        },
    },
    "additionalProperties": False,
    "required": ["node_relation", "primary_column"],
}


project_configuration_schema = {
    "$id": "project_configuration_schema",
    "type": "object",
    "properties": {
        "time_spine_table_configurations": {
            "type": "array",
            "items": {"$ref": "time_spine_table_configuration_schema"},
        },
        "time_spines": {
            "type": "array",
            "items": {"$ref": "time_spine_schema"},
        },
    },
    "additionalProperties": False,
    "required": [],
}

export_config_schema = {
    "$id": "export_config_schema",
    "type": "object",
    "properties": {
        "export_as": {"enum": export_destination_type_values},
        "schema": {"type": "string"},
        "alias": {"type": "string"},
    },
    "required": ["export_as"],
    "additionalProperties": False,
}


export_schema = {
    "$id": "export_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "config": {"$ref": "export_config_schema"},
    },
    "required": ["name", "config"],
    "additionalProperties": False,
}

saved_query_query_params_schema = {
    "$id": "saved_query_query_params_schema",
    "type": "object",
    "properties": {
        "metrics": {
            "type": "array",
            "items": {"type": "string"},
        },
        "group_by": {
            "type": "array",
            "items": {"type": "string"},
        },
        "order_by": {
            "type": "array",
            "items": {"type": "string"},
        },
        "limit": {"type": "integer"},
        "where": {"$ref": "filter_schema"},
    },
    "required": ["metrics"],
    "additionalProperties": False,
}

saved_query_schema = {
    "$id": "saved_query_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "description": {"type": "string"},
        "query_params": {"$ref": "saved_query_query_params_schema"},
        "label": {"type": "string"},
        "exports": {"type": "array", "items": {"$ref": "export_schema"}},
        "tags": {
            "oneOf": [
                {"type": "string"},
                {
                    "type": "array",
                    "items": {"type": "string"},
                },
            ],
        },
    },
    "required": ["name", "query_params"],
    "additionalProperties": False,
}

semantic_model_config_schema = {
    "$id": "semantic_model_config_schema",
    "type": "object",
    "properties": {
        "meta": {"type": "object", "propertyNames": {"type": "string"}},
    },
    "additionalProperties": False,
}

semantic_model_schema = {
    "$id": "semantic_model_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "node_relation": {"$ref": "node_relation_schema"},
        "defaults": {"$ref": "semantic_model_defaults_schema"},
        "primary_entity": {
            "type": "string",
        },
        "entities": {"type": "array", "items": {"$ref": "entity_schema"}},
        "measures": {"type": "array", "items": {"$ref": "measure_schema"}},
        "dimensions": {"type": "array", "items": {"$ref": "dimension_schema"}},
        "description": {"type": "string"},
        "label": {"type": "string"},
        "config": {"$ref": "semantic_model_config_schema"},
    },
    "additionalProperties": False,
    "required": ["name"],
}


schema_store = {
    # Top level schemas
    metric_schema["$id"]: metric_schema,
    semantic_model_schema["$id"]: semantic_model_schema,
    project_configuration_schema["$id"]: project_configuration_schema,
    saved_query_schema["$id"]: saved_query_schema,
    # Sub-object schemas
    filter_schema["$id"]: filter_schema,
    metric_input_measure_schema["$id"]: metric_input_measure_schema,
    metric_type_params_schema["$id"]: metric_type_params_schema,
    conversion_type_params_schema["$id"]: conversion_type_params_schema,
    cumulative_type_params_schema["$id"]: cumulative_type_params_schema,
    constant_property_input_schema["$id"]: constant_property_input_schema,
    entity_schema["$id"]: entity_schema,
    measure_schema["$id"]: measure_schema,
    dimension_schema["$id"]: dimension_schema,
    validity_params_schema["$id"]: validity_params_schema,
    dimension_type_params_schema["$id"]: dimension_type_params_schema,
    aggregation_type_params_schema["$id"]: aggregation_type_params_schema,
    non_additive_dimension_schema["$id"]: non_additive_dimension_schema,
    metric_input_schema["$id"]: metric_input_schema,
    node_relation_schema["$id"]: node_relation_schema,
    semantic_model_defaults_schema["$id"]: semantic_model_defaults_schema,
    time_spine_table_configuration_schema["$id"]: time_spine_table_configuration_schema,
    time_spine_schema["$id"]: time_spine_schema,
    custom_granularity_column_schema["$id"]: custom_granularity_column_schema,
    time_spine_primary_column_schema["$id"]: time_spine_primary_column_schema,
    export_schema["$id"]: export_schema,
    export_config_schema["$id"]: export_config_schema,
    saved_query_query_params_schema["$id"]: saved_query_query_params_schema,
    semantic_model_config_schema["$id"]: semantic_model_config_schema,
    metric_config_schema["$id"]: metric_config_schema,
    dimension_config_schema["$id"]: dimension_config_schema,
    entity_config_schema["$id"]: entity_config_schema,
    measure_config_schema["$id"]: measure_config_schema,
}

resources: List[Tuple[str, Resource]] = [(str(k), DRAFT7.create_resource(v)) for k, v in schema_store.items()]
registry: Registry = Registry().with_resources(resources)
semantic_model_validator = SchemaValidator(semantic_model_schema, registry=registry)
metric_validator = SchemaValidator(metric_schema, registry=registry)
project_configuration_validator = SchemaValidator(project_configuration_schema, registry=registry)
saved_query_validator = SchemaValidator(saved_query_schema, registry=registry)
