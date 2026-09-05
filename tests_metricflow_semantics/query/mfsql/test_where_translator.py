from __future__ import annotations

import textwrap

import pytest
import sqlglot
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.mfsql.where_translator import MfsqlWhereTranslationError, translate_where_clause
from metricflow_semantics.test_helpers.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)
from sqlglot import exp

from metricflow_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_semantic_manifest
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.parsing.where_filter.jinja_object_parser import JinjaObjectParser
from metricflow_semantic_interfaces.parsing.where_filter.parameter_set_factory import QueryItemLocation
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator


def _where_ast(sql: str) -> exp.Where:
    """Parse a full `SELECT ... WHERE ...` statement and return just the WHERE node."""
    where = sqlglot.parse_one(f"SELECT 1 {sql}").find(exp.Where)
    assert where is not None, f"Test SQL did not contain a WHERE clause: {sql}"
    return where


def _manifest_lookup_from_semantic_model_yaml(semantic_model_yaml: str) -> SemanticManifestLookup:
    yaml_file = YamlConfigFile(filepath="inline_for_test", contents=semantic_model_yaml)
    manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, yaml_file], apply_transformations=True
    ).semantic_manifest
    SemanticManifestValidator().checked_validations(manifest)
    return SemanticManifestLookup(manifest)


def test_translate_where_clause_covers_common_comparisons(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Bare dimension/entity/time-dimension columns across the common comparison operators."""
    where = _where_ast(
        """
        WHERE booking__is_instant = true
          AND metric_time__month >= '2020-01-01'
          AND ds IS NOT NULL
          AND (guest = 1 OR host = 2)
        """
    )

    result = translate_where_clause(where, simple_semantic_manifest_lookup)

    assert result == (
        "{{ Dimension('booking__is_instant') }} = TRUE AND {{ Dimension('metric_time__month') }} >= '2020-01-01' "
        "AND NOT {{ Dimension('ds') }} IS NULL AND ({{ Entity('guest') }} = 1 OR {{ Entity('host') }} = 2)"
    )


def test_translate_where_clause_is_accepted_by_the_real_where_filter_parser(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """The strongest correctness check: feed the translator's output into MetricFlow's actual filter parser."""
    where = _where_ast("WHERE booking__is_instant = true AND metric_time__month >= '2020-01-01'")

    result = translate_where_clause(where, simple_semantic_manifest_lookup)
    call_parameter_sets = JinjaObjectParser.parse_call_parameter_sets(
        where_sql_template=result,
        custom_granularity_names=tuple(simple_semantic_manifest_lookup.custom_granularities.keys()),
        query_item_location=QueryItemLocation.NON_ORDER_BY,
    )

    assert len(call_parameter_sets.dimension_call_parameter_sets) == 2
    assert {cps.dimension_reference.element_name for cps in call_parameter_sets.dimension_call_parameter_sets} == {
        "is_instant",
        "metric_time",
    }


def test_translate_where_clause_handles_extract_as_date_part(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """EXTRACT(<part> FROM <column>) becomes a chained `.date_part(...)` macro call."""
    where = _where_ast("WHERE EXTRACT(month FROM ds) = 3")

    result = translate_where_clause(where, simple_semantic_manifest_lookup)

    assert result == "{{ Dimension('ds').date_part('month') }} = 3"


def test_translate_where_clause_rejects_qualified_columns(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """There is no FROM-table namespace in mfsql, so a `t.column`-style reference should be rejected."""
    where = _where_ast("WHERE t.booking__is_instant = true")

    with pytest.raises(MfsqlWhereTranslationError, match="table-qualified"):
        translate_where_clause(where, simple_semantic_manifest_lookup)


def test_translate_where_clause_rejects_unknown_columns(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """A column that matches no dimension, entity, or time dimension in the manifest should be rejected."""
    where = _where_ast("WHERE definitely_not_a_real_dimension = 1")

    with pytest.raises(MfsqlWhereTranslationError, match="does not match a known"):
        translate_where_clause(where, simple_semantic_manifest_lookup)


def test_translate_where_clause_rejects_subqueries(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """A subquery inside an IN-list is real SQL, but outside the mfsql WHERE grammar's allowlist."""
    where = _where_ast("WHERE booking__is_instant IN (SELECT 1)")

    with pytest.raises(MfsqlWhereTranslationError, match="unsupported construct"):
        translate_where_clause(where, simple_semantic_manifest_lookup)


def test_translate_where_clause_rejects_unsupported_extract_part(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Only the DatePart-enum-backed EXTRACT parts (year/quarter/month/day/dow/doy) are supported."""
    where = _where_ast("WHERE EXTRACT(century FROM ds) = 3")

    with pytest.raises(MfsqlWhereTranslationError, match="Unsupported EXTRACT part"):
        translate_where_clause(where, simple_semantic_manifest_lookup)


_BOOKINGS_SOURCE_YAML_TEMPLATE = textwrap.dedent(
    """\
    semantic_model:
      name: bookings_source
      node_relation:
        schema_name: some_schema
        alias: bookings_source_table
      defaults:
        agg_time_dimension: ds
      measures:
        - name: bookings
          expr: "1"
          agg: sum
          create_metric: true
      dimensions:
        - name: ds
          type: time
          type_params:
            time_granularity: day
      {primary_entity_declaration}
    """
)


def test_translate_where_clause_bare_primary_entity_matches_metricflows_own_capability() -> None:
    """A bare primary-entity reference in mfsql behaves exactly like it would in a hand-written filter.

    Declaring a primary entity via the `primary_entity:` YAML shorthand never creates an `Entity` object on
    the semantic model at all (confirmed by inspecting `SemanticModel.entities`), so it's never registered as
    a linkable element anywhere in the manifest - not for mfsql, and not for a hand-written
    `{{ Entity('booking') }}` filter or `--group-by booking` either. Declaring the same entity explicitly via
    `entities: [{name: ..., type: primary}]` does register it, and both paths then succeed. This is a
    MetricFlow-wide behavior mfsql inherits, not a translator-specific gap.
    """
    shorthand_lookup = _manifest_lookup_from_semantic_model_yaml(
        _BOOKINGS_SOURCE_YAML_TEMPLATE.format(primary_entity_declaration="primary_entity: booking")
    )
    explicit_lookup = _manifest_lookup_from_semantic_model_yaml(
        _BOOKINGS_SOURCE_YAML_TEMPLATE.format(
            primary_entity_declaration="entities:\n        - name: booking\n          type: primary"
        )
    )

    with pytest.raises(MfsqlWhereTranslationError, match="does not match a known"):
        translate_where_clause(_where_ast("WHERE booking = 1"), shorthand_lookup)

    assert translate_where_clause(_where_ast("WHERE booking = 1"), explicit_lookup) == "{{ Entity('booking') }} = 1"
