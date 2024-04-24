from __future__ import annotations

from typing import Mapping

from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

from metricflow.validation.dataflow_join_validator import JoinDataflowOutputValidator
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


def test_natural_entity_instance_set_validation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests instance set validation for NATURAL target entity types.

    These tests rely on the scd_semantic_manifest_lookup, which makes extensive use of NATURAL key types.
    """
    natural_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST]
        .data_set_mapping["primary_accounts"]
        .instance_set
    )
    primary_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].data_set_mapping["users_latest"].instance_set
    )
    foreign_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST]
        .data_set_mapping["bookings_source"]
        .instance_set
    )
    unique_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].data_set_mapping["companies"].instance_set
    )
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = JoinDataflowOutputValidator(
        semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup
    )

    # Valid cases
    natural_primary = join_evaluator.is_valid_instance_set_join(
        left_instance_set=natural_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    natural_unique = join_evaluator.is_valid_instance_set_join(
        left_instance_set=natural_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    foreign_natural = join_evaluator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=natural_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    primary_natural = join_evaluator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=natural_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    unique_natural = join_evaluator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=natural_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    # Invalid cases
    natural_foreign = join_evaluator.is_valid_instance_set_join(
        left_instance_set=natural_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    natural_natural = join_evaluator.is_valid_instance_set_join(
        left_instance_set=natural_user_instance_set,
        right_instance_set=natural_user_instance_set,
        on_entity_reference=user_entity_reference,
    )

    valid_joins = {
        "natural to primary": natural_primary,
        "natural to unique": natural_unique,
        "foreign to natural": foreign_natural,
        "primary to natural": primary_natural,
        "unique to natural": unique_natural,
    }
    invalid_joins = {
        "natural to foreign": natural_foreign,
        "natural to natural": natural_natural,
    }
    assert all(valid_joins.values()) and not any(invalid_joins.values()), (
        f"Found unexpected join validator results when validating joins involving natural key comparisons! Valid "
        f"joins marked invalid: {[k for k,v in valid_joins.items() if not v]}. Invalid joins marked valid: "
        f"{[k for k, v in invalid_joins.items() if v]}."
    )


def test_distinct_target_instance_set_join_validation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests instance set join validation to a PRIMARY or UNIQUE entity."""
    foreign_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
        .data_set_mapping["listings_latest"]
        .instance_set
    )
    primary_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
        .data_set_mapping["users_latest"]
        .instance_set
    )
    unique_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping["companies"].instance_set
    )
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = JoinDataflowOutputValidator(
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup
    )

    foreign_primary = join_evaluator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    primary_primary = join_evaluator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    unique_primary = join_evaluator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    foreign_unique = join_evaluator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    primary_unique = join_evaluator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    unique_unique = join_evaluator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_entity_reference=user_entity_reference,
    )

    results = {
        "foreign to primary": foreign_primary,
        "primary to primary": primary_primary,
        "unique to primary": unique_primary,
        "foreign to unique": foreign_unique,
        "primary to unique": primary_unique,
        "unique to unique": unique_unique,
    }
    assert all(results.values()), (
        f"All instance set level join types for primary and unique targets should be valid, but we found "
        f"at least one that was not! Incorrectly failing types: {[k for k,v in results.items() if not v]}."
    )


def test_foreign_target_instance_set_join_validation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests semantic model join validation to FOREIGN entity types."""
    foreign_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
        .data_set_mapping["listings_latest"]
        .instance_set
    )
    primary_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
        .data_set_mapping["users_latest"]
        .instance_set
    )
    unique_user_instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping["companies"].instance_set
    )
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = JoinDataflowOutputValidator(
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup
    )

    foreign_foreign = join_evaluator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    primary_foreign = join_evaluator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_entity_reference=user_entity_reference,
    )
    unique_foreign = join_evaluator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_entity_reference=user_entity_reference,
    )

    results = {
        "foreign to foreign": foreign_foreign,
        "primary to foreign": primary_foreign,
        "unique to foreign": unique_foreign,
    }
    assert not any(results.values()), (
        f"All semantic model level joins against foreign targets should be invalid, but we found at least one "
        f"that was not! Incorrectly passing types: {[k for k,v in results.items() if v]}."
    )
