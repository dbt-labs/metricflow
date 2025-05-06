from __future__ import annotations

from typing import Mapping

import pytest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.instances import InstanceSet
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.nodes.join_to_base import ValidityWindowJoinDescription
from metricflow.plan_conversion.instance_set_transforms.instance_converters import CreateValidityWindowJoinDescription
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


def test_no_validity_dims(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests converting an instance set with no matching dimensions to a ValidityWindowJoinDescription."""
    # bookings_source is a fact table, and has no validity window dimensions
    dataset = mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].data_set_mapping["bookings_source"]

    validity_window_join_description = CreateValidityWindowJoinDescription(
        semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup
    ).transform(instance_set=dataset.instance_set)

    assert validity_window_join_description is None, (
        f"We managed to create a validity window join description `{validity_window_join_description}` from a "
        f"semantic model that does not have one defined!"
    )


def test_validity_window_conversion(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests converting an instance set with a single validity window into a ValidityWindowJoinDescription."""
    # The listings semantic model uses a 2-column SCD Type II layout
    dataset = mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].data_set_mapping["listings"]
    expected_join_description = ValidityWindowJoinDescription(
        window_start_dimension=TimeDimensionSpec(
            element_name="window_start",
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            entity_links=(),
        ),
        window_end_dimension=TimeDimensionSpec(
            element_name="window_end",
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            entity_links=(),
        ),
    )

    validity_window_join_description = CreateValidityWindowJoinDescription(
        semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup
    ).transform(instance_set=dataset.instance_set)

    assert (
        validity_window_join_description is not None
    ), "Failed to make a validity window join description from a dataset which should have one configured!"
    assert (
        validity_window_join_description == expected_join_description
    ), f"Expected validity window: `{expected_join_description}` but got: `{validity_window_join_description}`"


def test_multiple_validity_windows(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the behavior of this converter when it encounters an instance set with multiple validity windows."""
    first_dataset = mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].data_set_mapping["listings"]
    second_dataset = mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].data_set_mapping[
        "primary_accounts"
    ]
    merged_instance_set = InstanceSet.merge([first_dataset.instance_set, second_dataset.instance_set])
    with pytest.raises(AssertionError, match="Found more than 1 set of validity window specs"):
        CreateValidityWindowJoinDescription(
            semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup
        ).transform(merged_instance_set)
