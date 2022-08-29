import json
from copy import deepcopy

from pathlib import Path
from typing import Dict, List, Union

from metricflow.model.parsing import schemas_internal

TOP_LEVEL_SCHEMAS = {
    "metric": "metric",
    "data_source": "data_source",
    "derived_group_by_element_schema": "derived_identifier",
    "materialization_schema": "materialization",
}

BASE_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "MetricFlow file schema",
}


def generate_explict_schema(schema_store: Dict) -> Dict:
    """Generates a single json schema object from the given schema store."""
    ref_to_definition_mapping = {key: f"#/definitions/{key}" for key in schema_store.keys()}
    definitions = {}
    for schema_name, _schema in schema_store.items():
        schema = deepcopy(_schema)

        rewritten_schema = _rewrite_refs(schema, ref_to_definition_mapping)
        assert isinstance(rewritten_schema, dict)

        if "definitions" in rewritten_schema:
            nested_definitions = rewritten_schema["definitions"]
            for name in nested_definitions.keys():
                definitions[name] = nested_definitions[name]
            rewritten_schema.pop("definitions", None)

        definitions[schema_name] = rewritten_schema

    properties = {}
    for schema_name, object_name in TOP_LEVEL_SCHEMAS.items():
        properties[object_name] = {"$ref": ref_to_definition_mapping[schema_name]}

    full_schema: Dict = deepcopy(BASE_SCHEMA)
    full_schema["properties"] = properties
    full_schema["definitions"] = definitions

    return full_schema


def _rewrite_refs(obj: Union[Dict, List, bool, str], mapping: Dict) -> Union[Dict, List, bool, str]:
    """Replaces the $refs from their names to their definition section identifiers."""
    if isinstance(obj, dict):
        _dict = {}
        for k, v in obj.items():
            if k == "$ref" and v in mapping:
                _dict[k] = mapping[v]
            else:
                _dict[k] = _rewrite_refs(v, mapping)
        return _dict
    if isinstance(obj, list):
        _list = []
        for element in obj:
            _list.append(_rewrite_refs(element, mapping))
        return _list
    return obj


def write_schema(schema: Dict, output_dir: str, file_name: str) -> None:
    """Writes the schema from the specified schema store to the given path"""

    path = Path(output_dir).resolve()
    path.mkdir(exist_ok=True)
    with open(path / file_name, "w") as f:
        json.dump(schema, f, indent=4, sort_keys=True)


if __name__ == "__main__":
    schema = generate_explict_schema(schemas_internal.schema_store)
    output_dir = str(Path(__file__).parent / "schemas")
    write_schema(schema, output_dir, "metricflow.json")
