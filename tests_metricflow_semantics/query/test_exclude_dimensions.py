from __future__ import annotations

import textwrap
from typing import Sequence

import pytest
from metricflow_semantics.errors.error_classes import InvalidQueryException
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics import metric_lookup as metric_lookup_module
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)

from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.references import MetricReference
from tests_metricflow_semantics.query.conftest import create_manifest_from_yaml_files

TICKETS_AND_CUSTOMERS_YAML = textwrap.dedent(
    """\
    semantic_model:
      name: tickets_source
      node_relation:
        schema_name: some_schema
        alias: tickets_table
      defaults:
        agg_time_dimension: ds
      measures:
        - name: tickets
          expr: "1"
          agg: sum
          create_metric: true
      dimensions:
        - name: category
          type: categorical
        - name: ds
          type: time
          type_params:
            time_granularity: day
      primary_entity: ticket
    ---
    semantic_model:
      name: customers_source
      node_relation:
        schema_name: some_schema
        alias: customers_table
      defaults:
        agg_time_dimension: ds
      measures:
        - name: customers
          expr: "1"
          agg: sum
          create_metric: true
      dimensions:
        - name: segment
          type: categorical
        - name: ds
          type: time
          type_params:
            time_granularity: day
      primary_entity: customer
    ---
    metric:
      name: tickets_per_customer_excluding_category
      type: ratio
      type_params:
        numerator:
          name: tickets
        denominator:
          name: customers
          exclude_dimensions: [category]
    ---
    metric:
      name: tickets_per_customer_excluding_mismatched_entity_category
      type: ratio
      type_params:
        numerator:
          name: tickets
        denominator:
          name: customers
          exclude_dimensions: [customer__category]
    ---
    metric:
      name: tickets_plus_customers_excluding_qualified_category
      type: derived
      type_params:
        expr: tickets + customers
        metrics:
          - name: tickets
          - name: customers
            exclude_dimensions: [ticket__category]
    ---
    metric:
      name: tickets_per_customer_excluding_segment
      type: ratio
      type_params:
        numerator:
          name: tickets
        denominator:
          name: customers
          exclude_dimensions: [segment]
    ---
    metric:
      name: tickets_per_customer
      type: ratio
      type_params:
        numerator:
          name: tickets
        denominator:
          name: customers
    ---
    metric:
      name: tickets_plus_customers_excluding_category
      type: derived
      type_params:
        expr: tickets + customers
        metrics:
          - name: tickets
          - name: customers
            exclude_dimensions: [category]
    ---
    metric:
      name: tickets_per_customer_nested_exclusion
      type: ratio
      type_params:
        numerator:
          name: tickets
        denominator:
          name: tickets_plus_customers_excluding_category
    """
)


_NO_MATCH_FOR = r"does not match any of the available group-by-items for\s+SimpleMetric\('{metric}'\)"


@pytest.fixture(scope="module")
def manifest_lookup() -> SemanticManifestLookup:  # noqa: D103
    yaml_file = YamlConfigFile(filepath="inline_for_test_exclude_dimensions", contents=TICKETS_AND_CUSTOMERS_YAML)
    return SemanticManifestLookup(
        create_manifest_from_yaml_files([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, yaml_file])
    )


@pytest.fixture(scope="module")
def query_parser(manifest_lookup: SemanticManifestLookup) -> MetricFlowQueryParser:  # noqa: D103
    return MetricFlowQueryParser(semantic_manifest_lookup=manifest_lookup)


def _resolved_dunder_names(query_parser: MetricFlowQueryParser, metric_name: str, group_by_name: str) -> Sequence[str]:
    result = query_parser.parse_and_validate_query(metric_names=(metric_name,), group_by_names=(group_by_name,))
    return tuple(spec.dunder_name for spec in result.query_spec.linkable_specs)


DECLARING_CATEGORY = [
    "tickets_per_customer_excluding_category",
    "tickets_per_customer_nested_exclusion",
    "tickets_plus_customers_excluding_qualified_category",
]


@pytest.mark.parametrize("metric_name", DECLARING_CATEGORY)
def test_declared_dimension_resolves_without_the_declaring_input_having_it(  # noqa: D103
    query_parser: MetricFlowQueryParser, metric_name: str
) -> None:
    assert _resolved_dunder_names(query_parser, metric_name, "ticket__category") == ("ticket__category",)


def test_dimension_only_the_declaring_input_has_is_still_an_error(  # noqa: D103
    query_parser: MetricFlowQueryParser,
) -> None:
    with pytest.raises(InvalidQueryException, match=_NO_MATCH_FOR.format(metric="tickets")):
        query_parser.parse_and_validate_query(
            metric_names=("tickets_per_customer_excluding_category",), group_by_names=("customer__segment",)
        )


def test_declaring_another_dimension_does_not_relax_this_one(query_parser: MetricFlowQueryParser) -> None:  # noqa: D103
    with pytest.raises(InvalidQueryException, match=_NO_MATCH_FOR.format(metric="customers")):
        query_parser.parse_and_validate_query(
            metric_names=("tickets_per_customer_excluding_segment",), group_by_names=("ticket__category",)
        )


def _common_dunder_names(manifest_lookup: SemanticManifestLookup, *metric_names: str) -> set[str]:
    return {
        spec.dunder_name
        for spec in manifest_lookup.metric_lookup.get_common_group_by_items(
            metric_references=tuple(MetricReference(metric_name) for metric_name in metric_names)
        ).specs
    }


@pytest.mark.parametrize("metric_name", DECLARING_CATEGORY)
def test_declared_dimension_is_listed_as_a_group_by_item(  # noqa: D103
    manifest_lookup: SemanticManifestLookup, metric_name: str
) -> None:
    assert "ticket__category" in _common_dunder_names(manifest_lookup, metric_name)


def test_declared_name_with_entity_links_that_do_not_match_does_not_relax(  # noqa: D103
    query_parser: MetricFlowQueryParser, manifest_lookup: SemanticManifestLookup
) -> None:
    metric_name = "tickets_per_customer_excluding_mismatched_entity_category"
    with pytest.raises(InvalidQueryException, match=_NO_MATCH_FOR.format(metric="customers")):
        query_parser.parse_and_validate_query(metric_names=(metric_name,), group_by_names=("ticket__category",))
    assert "ticket__category" not in _common_dunder_names(manifest_lookup, metric_name)


def test_dimension_not_declared_is_not_listed(manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D103
    assert "customer__segment" not in _common_dunder_names(manifest_lookup, "tickets_per_customer_excluding_category")
    assert "ticket__category" not in _common_dunder_names(manifest_lookup, "tickets_per_customer")
    assert "ticket__category" not in _common_dunder_names(manifest_lookup, "tickets_per_customer_excluding_segment")
    assert "ticket__category" not in _common_dunder_names(manifest_lookup, "customers")


def test_listing_for_several_metrics_intersects_the_results(  # noqa: D103
    manifest_lookup: SemanticManifestLookup,
) -> None:
    names = _common_dunder_names(manifest_lookup, "tickets_per_customer_excluding_category", "customers")
    assert "ticket__category" not in names
    assert "metric_time__day" in names


def _stacked_derived_metrics_yaml(depth: int) -> str:
    """Each metric uses the metric of the previous level for both of its inputs."""
    metric_yamls = []
    input_name = "tickets_per_customer_excluding_category"
    for level in range(1, depth + 1):
        metric_name = f"stacked_{level}"
        metric_yamls.append(
            textwrap.dedent(
                f"""\
                metric:
                  name: {metric_name}
                  type: derived
                  type_params:
                    expr: left_input + right_input
                    metrics:
                      - name: {input_name}
                        alias: left_input
                      - name: {input_name}
                        alias: right_input
                """
            )
        )
        input_name = metric_name
    return "---\n".join(metric_yamls)


def test_group_by_items_of_stacked_metrics_with_exclusions_are_resolved_once_per_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metrics that share inputs must not make the resolution repeat the work of the shared inputs."""
    depth = 10
    yaml_file = YamlConfigFile(
        filepath="inline_for_test_stacked_exclusions",
        contents=TICKETS_AND_CUSTOMERS_YAML + "---\n" + _stacked_derived_metrics_yaml(depth),
    )
    lookup = SemanticManifestLookup(
        create_manifest_from_yaml_files([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, yaml_file])
    )
    call_count = 0
    original = metric_lookup_module.with_excluded_dimensions_allowed

    def counting_wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]  # noqa: ANN002, ANN003
        nonlocal call_count
        call_count += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(metric_lookup_module, "with_excluded_dimensions_allowed", counting_wrapper)

    group_by_items = lookup.metric_lookup.get_common_group_by_items([MetricReference(f"stacked_{depth}")])

    assert group_by_items is not None
    assert call_count <= depth + 1
