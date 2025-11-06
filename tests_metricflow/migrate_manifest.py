from __future__ import annotations

import logging
import textwrap
from collections.abc import Mapping, Set
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import yaml
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.type_enums import MetricType
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    """Custom representer for strings to use literal block style for multi-line strings.

    Args:
        dumper: The YAML dumper instance.
        data: The string data to represent.

    Returns:
        A YAML scalar node with appropriate style for the string.
    """
    if "\n" in data:
        # Use literal block style (|) for multi-line strings
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    # Use default representation for single-line strings
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def _filter_excluded_fields(
    data: Any, excluded_field_names: Set[str], exclude_null_fields: bool, exclude_false_fields: bool
) -> Any:
    """Recursively filter out excluded field names from nested data structures.

    Args:
        data: The data structure to filter (can be dict, list, or primitive).
        excluded_field_names: A set of field names to exclude.
        exclude_null_fields: If True, exclude fields with None values.
        exclude_false_fields: If True, exclude fields with False boolean values.

    Returns:
        The filtered data structure with excluded fields removed at all nesting levels.
        Enum values are converted to their underlying values.
    """
    # Convert Enum values to their actual values
    # logger.info(f"{data=}")
    if isinstance(data, Enum):
        return data.value
    elif isinstance(data, dict):
        # For dictionaries, filter out excluded keys and recursively process values
        filtered = {}
        for key, value in data.items():
            # Skip excluded field names
            if key in excluded_field_names:
                continue
            # Skip None values if exclude_null_fields is True
            if exclude_null_fields and value is None:
                continue
            # Skip False values if exclude_false_fields is True
            if exclude_false_fields and (value is False or value == []):
                continue

            # Recursively process the value
            filtered[key] = _filter_excluded_fields(
                value, excluded_field_names, exclude_null_fields, exclude_false_fields
            )

        return filtered
    elif isinstance(data, list):
        # For lists, recursively process each item
        return [
            _filter_excluded_fields(item, excluded_field_names, exclude_null_fields, exclude_false_fields)
            for item in data
        ]
    else:
        # For primitive values, return as is
        return data


def _apply_dict_transform(data: Any, dict_transform: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> Any:
    if isinstance(data, Enum):
        return data.value
    elif isinstance(data, dict):
        return dict_transform({key: _apply_dict_transform(value, dict_transform) for key, value in data.items()})
    elif isinstance(data, list):
        # For lists, recursively process each item
        return [_apply_dict_transform(item, dict_transform) for item in data]
    else:
        # For primitive values, return as is
        return data


def pydantic_to_yaml_str(
    base_model: BaseModel,
    excluded_field_names: Set[str] = frozenset(()),
    exclude_null_fields: bool = True,
    exclude_false_fields: bool = True,
    dict_transform: Optional[Callable[[Mapping[str, Any]], Mapping[str, Any]]] = None,
) -> str:
    """Convert a Pydantic model to YAML.

    Args:
        base_model: The Pydantic model to convert to YAML.
        excluded_field_names: A set of field names to exclude from the YAML output.
            This applies recursively to all nested objects.
        exclude_null_fields: If True, exclude fields with None values from the output.
            Defaults to True.
        exclude_false_fields: If True, exclude fields with False boolean values from the output.
            Defaults to True.

    Returns:
        A YAML string representation of the model, with excluded fields removed.
        Enum values are serialized as their underlying values.
        Multi-line strings always use the literal block style (|), never quotes.
        Single-line string values use double quotes when quoting is required.
        List items are indented relative to their parent key.
    """
    # Convert the Pydantic model to a dictionary
    model_dict = base_model.dict()

    # Recursively remove excluded fields and optionally null/false fields from the dictionary
    filtered_dict = _filter_excluded_fields(model_dict, excluded_field_names, exclude_null_fields, exclude_false_fields)
    if dict_transform is not None:
        filtered_dict = _apply_dict_transform(filtered_dict, dict_transform)

    # Create a custom Dumper class with the string representer and double quote preference
    class CustomDumper(yaml.Dumper):
        def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
            """Override to indent list items.

            This ensures that list items are indented relative to their parent key.
            """
            return super().increase_indent(flow=flow, indentless=False)

        def analyze_scalar(self, scalar: str) -> Any:
            """Override to force block style for multi-line strings.

            This ensures multi-line strings use literal block style (|) even if they
            contain tabs, unicode, trailing spaces, or other special characters that
            would normally prevent PyYAML from using block style.
            """
            analysis = super().analyze_scalar(scalar)
            # If the string contains newlines, force allow_block to True
            if "\n" in scalar:
                analysis.allow_block = True
            return analysis

        def choose_scalar_style(self) -> Any:
            """Override to prefer double quotes over single quotes for string values.

            Preserves literal block style (|) for multi-line strings.
            """
            style = super().choose_scalar_style()
            # Preserve literal (|) and folded (>) styles for multi-line strings
            if style in ("|", ">"):
                return style
            # If the default style would be single quotes ('), use double quotes (") instead
            if style == "'":
                return '"'
            return style

    CustomDumper.add_representer(str, _str_representer)

    # Convert the dictionary to a YAML string
    # Use default_flow_style=False for block style (more readable)
    # Use sort_keys=False to preserve field order
    # Use a large width to prevent automatic line wrapping of long strings
    yaml_str = yaml.dump(
        filtered_dict,
        Dumper=CustomDumper,
        default_flow_style=False,
        sort_keys=False,
        width=1000000,  # Very large width to prevent line wrapping
    )

    return yaml_str


def _metric_dict_transform(input_dict: Mapping[str, Any]) -> Mapping[str, Any]:
    output_dict = {}
    for key, value in input_dict.items():
        # logger.info(f"{key=} {value=}")
        if key == "filter":
            assert isinstance(value, Mapping)
            where_filters = value["where_filters"]
            assert isinstance(where_filters, Sequence)
            if len(where_filters) == 1:
                value = where_filters[0]["where_sql_template"]
            else:
                value = [where_filter["where_sql_template"] for where_filter in where_filters]

        if key == "offset_window" or key == "window":
            assert isinstance(value, Mapping)
            count = value["count"]
            granularity = value["granularity"]
            if count > 1:
                value = f"{count} {granularity}s"
            else:
                value = f"{count} {granularity}"

        if key == "type_params":
            assert isinstance(value, Mapping)
            # logger.info(f"Pre filter {value=}")
            value = {_key: _value for _key, _value in value.items() if _key != "window"}
            # logger.info(f"Post filter {value=}")
        # logger.info(f"{key=} {value=}")
        output_dict[key] = value
    return output_dict


def _semantic_model_dict_transform(input_dict: Mapping[str, Any]) -> Mapping[str, Any]:
    output_dict = {}
    for key, value in input_dict.items():
        # logger.info(f"{key=} {value=}")
        if key == "schema_name":
            value = "$source_schema"
        output_dict[key] = value
    return output_dict


def test_dump_metric(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    excluded_field_names = {"metadata", "is_private", "input_measures", "measure"}

    for metric in simple_semantic_manifest.metrics:
        if metric.name == "trailing_2_months_revenue":
            logger.info(
                f"Output:\n{pydantic_to_yaml_str(metric, excluded_field_names=excluded_field_names, dict_transform=_metric_dict_transform)}"
            )


def mf_indent(text: str) -> str:
    return textwrap.indent(text, prefix="  ")


def test_dump_metrics_yaml(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    excluded_field_names = {"metadata", "is_private", "input_measures", "measure", "base_measure", "conversion_measure"}

    metric_yaml_strs: list[str] = []
    for metric in simple_semantic_manifest.metrics:
        metric_yaml_strs.append(
            "metric:\n"
            + mf_indent(
                pydantic_to_yaml_str(
                    metric, excluded_field_names=excluded_field_names, dict_transform=_metric_dict_transform
                )
            )
        )

    with open(
        "metricflow-semantics/metricflow_semantics/test_helpers/semantic_manifest_yamls/simple_manifest/metrics.yaml",
        "w",
    ) as fp:
        fp.write("---\n")
        fp.write("---\n".join(metric_yaml_strs))


def test_dump_semantic_models_yaml(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    excluded_field_names = {"metadata", "relation_name", "measures", "label"}

    for semantic_model in simple_semantic_manifest.semantic_models:
        semantic_model_yaml_str = "\n".join(
            [
                "---",
                "semantic_model:",
                mf_indent(pydantic_to_yaml_str(semantic_model, excluded_field_names=excluded_field_names)),
            ]
        )

        with open(
            f"tests/fixtures/semantic_manifest_yamls/simple_semantic_manifest"
            f"/semantic_models/{semantic_model.name}.yaml",
            "w",
        ) as fp:
            fp.write(semantic_model_yaml_str)


def test_migrate_manifest(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    semantic_manifest = simple_semantic_manifest
    metric_excluded_field_names = {"metadata", "input_measures", "measure", "base_measure", "conversion_measure"}

    metric_yaml_strs: list[str] = []
    for metric in semantic_manifest.metrics:
        if metric.type is MetricType.SIMPLE:
            continue
        metric_yaml_strs.append(
            "metric:\n"
            + mf_indent(
                pydantic_to_yaml_str(
                    metric, excluded_field_names=metric_excluded_field_names, dict_transform=_metric_dict_transform
                )
            )
        )

    manifest_root = Path(
        "metricflow-semantics/metricflow_semantics/test_helpers/semantic_manifest_yamls/simple_manifest"
    )

    with open(manifest_root.joinpath("metrics.yaml"), "w") as fp:
        fp.write("---\n")
        fp.write("---\n".join(metric_yaml_strs))

    model_excluded_field_names = {"metadata", "relation_name", "measures", "label"}
    for semantic_model in semantic_manifest.semantic_models:
        semantic_model_yaml_lines = [
            "---",
            "semantic_model:",
            mf_indent(
                pydantic_to_yaml_str(
                    semantic_model,
                    excluded_field_names=model_excluded_field_names,
                    dict_transform=_semantic_model_dict_transform,
                )
            ),
        ]

        for metric in semantic_manifest.metrics:
            if metric.type is not MetricType.SIMPLE:
                continue
            metric_aggregation_params = metric.type_params.metric_aggregation_params
            if metric_aggregation_params is None:
                continue

            if metric_aggregation_params.semantic_model != semantic_model.name:
                continue

            semantic_model_yaml_lines.append("---")
            semantic_model_yaml_lines.append("metric:")
            semantic_model_yaml_lines.append(
                mf_indent(
                    pydantic_to_yaml_str(
                        metric, excluded_field_names=metric_excluded_field_names, dict_transform=_metric_dict_transform
                    )
                )
            )

        with open(
            manifest_root.joinpath(f"semantic_models/{semantic_model.name}.yaml"),
            "w",
        ) as fp:
            fp.write("\n".join(semantic_model_yaml_lines))
