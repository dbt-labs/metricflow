from __future__ import annotations

from typing import Dict, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.entity import EntityType
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.model.semantics.semantic_model_join_evaluator import (
    SemanticModelEntityJoin,
    SemanticModelEntityJoinType,
    SemanticModelJoinEvaluator,
    SemanticModelLink,
)
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository


def _get_join_types_for_entity_type(entity_type: EntityType) -> Sequence[SemanticModelEntityJoinType]:
    """Exhaustively evaluate entity types and return a sequence of all possible join type pairs.

    The exhaustive conditional statically enforces that every entity type is handled on the left.
    The complete set of matching join types ensures that all pairs are used.
    """
    if (
        entity_type is EntityType.FOREIGN
        or entity_type is EntityType.PRIMARY
        or entity_type is EntityType.UNIQUE
        or entity_type is EntityType.NATURAL
    ):
        join_types = tuple(
            SemanticModelEntityJoinType(left_entity_type=entity_type, right_entity_type=join_type)
            for join_type in EntityType
        )
        return join_types
    else:
        assert_values_exhausted(entity_type)


def test_join_type_coverage() -> None:
    """Ensures all entity type pairs are handled somewhere in the valid/invalid join mapping sets.

    This will prevent surprise RuntimeErrors in production by raising static exceptions for unhandled entity types
    and triggering a test failure for types which are handled in a non-exhaustive fashion
    """
    all_join_types = set(
        SemanticModelJoinEvaluator._INVALID_ENTITY_JOINS + SemanticModelJoinEvaluator._VALID_ENTITY_JOINS
    )
    for entity_type in EntityType:
        join_types = _get_join_types_for_entity_type(entity_type=entity_type)
        for join_type in join_types:
            assert (
                join_type in all_join_types
            ), f"Unhandled entity join type {join_type} not in valid or invalid entity join lists!"


def __get_simple_model_user_semantic_model_references_by_type(
    semantic_manifest_lookup: SemanticManifestLookup,
) -> Dict[EntityType, SemanticModelReference]:
    """Helper to get a set of semantic models with the `user` identifier organized by identifier type."""
    foreign_user_semantic_model = semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("listings_latest")
    )
    primary_user_semantic_model = semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("users_latest")
    )
    unique_user_semantic_model = semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("companies")
    )

    assert foreign_user_semantic_model, "Could not find `listings_latest` semantic model in simple model!"
    assert primary_user_semantic_model, "Could not find `users_latest` semantic model in simple model!"
    assert unique_user_semantic_model, "Could not find `companies` semantic model in simple model!"

    return {
        EntityType.FOREIGN: foreign_user_semantic_model.reference,
        EntityType.PRIMARY: primary_user_semantic_model.reference,
        EntityType.UNIQUE: unique_user_semantic_model.reference,
    }


def test_distinct_target_semantic_model_join_validation(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests semantic model join validation to a PRIMARY or UNIQUE entity.

    PRIMARY and UNIQUE entity targets should be valid for any join at the semantic model level because they both
    represent entity columns with distinct value sets, and as such there is no risk of inadvertent fanout joins.
    """
    semantic_model_references = __get_simple_model_user_semantic_model_references_by_type(
        simple_semantic_manifest_lookup
    )
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = SemanticModelJoinEvaluator(
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup
    )

    foreign_primary = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.FOREIGN],
        right_semantic_model_reference=semantic_model_references[EntityType.PRIMARY],
        on_entity_reference=user_entity_reference,
    )
    primary_primary = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.PRIMARY],
        right_semantic_model_reference=semantic_model_references[EntityType.PRIMARY],
        on_entity_reference=user_entity_reference,
    )
    unique_primary = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.UNIQUE],
        right_semantic_model_reference=semantic_model_references[EntityType.PRIMARY],
        on_entity_reference=user_entity_reference,
    )
    foreign_unique = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.FOREIGN],
        right_semantic_model_reference=semantic_model_references[EntityType.UNIQUE],
        on_entity_reference=user_entity_reference,
    )
    primary_unique = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.PRIMARY],
        right_semantic_model_reference=semantic_model_references[EntityType.UNIQUE],
        on_entity_reference=user_entity_reference,
    )
    unique_unique = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.UNIQUE],
        right_semantic_model_reference=semantic_model_references[EntityType.UNIQUE],
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
        f"All semantic model level join types for primary and unique targets should be valid, but we found "
        f"at least one that was not! Incorrectly failing types: {[k for k,v in results.items() if not v]}."
    )


def test_foreign_target_semantic_model_join_validation(simple_semantic_manifest_lookup: SemanticManifestLookup) -> None:
    """Tests semantic model join validation to FOREIGN entity types.

    These should all fail by default, as fanout joins are not supported
    """
    semantic_model_references = __get_simple_model_user_semantic_model_references_by_type(
        simple_semantic_manifest_lookup
    )
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = SemanticModelJoinEvaluator(
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup
    )

    foreign_foreign = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.FOREIGN],
        right_semantic_model_reference=semantic_model_references[EntityType.FOREIGN],
        on_entity_reference=user_entity_reference,
    )
    primary_foreign = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.PRIMARY],
        right_semantic_model_reference=semantic_model_references[EntityType.FOREIGN],
        on_entity_reference=user_entity_reference,
    )
    unique_foreign = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=semantic_model_references[EntityType.UNIQUE],
        right_semantic_model_reference=semantic_model_references[EntityType.FOREIGN],
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


def test_semantic_model_join_validation_on_missing_entity(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests semantic model join validation where the entity is missing from one or both semantic models."""
    primary_listing_semantic_model = simple_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("listings_latest")
    )
    assert primary_listing_semantic_model, "Could not find semantic model `listings_latest` in the simple model!"
    no_listing_semantic_model = simple_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("id_verifications")
    )
    assert no_listing_semantic_model, "Could not find semantic model `id_verifications` in the simple model!"
    listing_entity_reference = EntityReference(element_name="listing")
    join_evaluator = SemanticModelJoinEvaluator(
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup
    )

    assert not join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=no_listing_semantic_model.reference,
        right_semantic_model_reference=primary_listing_semantic_model.reference,
        on_entity_reference=listing_entity_reference,
    ), (
        "Found valid join on `listing` involving the `id_verifications` semantic model, which does not include the "
        "`listing` entity!"
    )


def test_distinct_target_instance_set_join_validation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests instance set join validation to a PRIMARY or UNIQUE entity."""
    foreign_user_instance_set = consistent_id_object_repository.simple_model_data_sets["listings_latest"].instance_set
    primary_user_instance_set = consistent_id_object_repository.simple_model_data_sets["users_latest"].instance_set
    unique_user_instance_set = consistent_id_object_repository.simple_model_data_sets["companies"].instance_set
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = SemanticModelJoinEvaluator(
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
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests semantic model join validation to FOREIGN entity types."""
    foreign_user_instance_set = consistent_id_object_repository.simple_model_data_sets["listings_latest"].instance_set
    primary_user_instance_set = consistent_id_object_repository.simple_model_data_sets["users_latest"].instance_set
    unique_user_instance_set = consistent_id_object_repository.simple_model_data_sets["companies"].instance_set
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = SemanticModelJoinEvaluator(
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


def test_get_joinable_semantic_models_single_hop(  # noqa: D
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    semantic_model_reference = SemanticModelReference(semantic_model_name="account_month_txns")
    join_evaluator = SemanticModelJoinEvaluator(
        semantic_model_lookup=multi_hop_join_semantic_manifest_lookup.semantic_model_lookup
    )

    # Single-hop
    joinable_semantic_models = join_evaluator.get_joinable_semantic_models(
        left_semantic_model_reference=semantic_model_reference
    )
    assert set(joinable_semantic_models.keys()) == {"bridge_table"}
    assert joinable_semantic_models["bridge_table"] == SemanticModelLink(
        left_semantic_model_reference=SemanticModelReference(semantic_model_name="account_month_txns"),
        join_path=[
            SemanticModelEntityJoin(
                right_semantic_model_reference=SemanticModelReference(semantic_model_name="bridge_table"),
                entity_reference=EntityReference(element_name="account_id"),
                join_type=SemanticModelEntityJoinType(
                    left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.UNIQUE
                ),
            )
        ],
    )


def test_get_joinable_semantic_models_multi_hop(  # noqa: D
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    semantic_model_reference = SemanticModelReference(semantic_model_name="account_month_txns")
    join_evaluator = SemanticModelJoinEvaluator(
        semantic_model_lookup=multi_hop_join_semantic_manifest_lookup.semantic_model_lookup
    )

    # 2-hop
    joinable_semantic_models = join_evaluator.get_joinable_semantic_models(
        left_semantic_model_reference=semantic_model_reference, include_multi_hop=True
    )
    assert set(joinable_semantic_models.keys()) == {"bridge_table", "customer_other_data", "customer_table"}
    assert joinable_semantic_models["bridge_table"] == SemanticModelLink(
        left_semantic_model_reference=SemanticModelReference(semantic_model_name="account_month_txns"),
        join_path=[
            SemanticModelEntityJoin(
                right_semantic_model_reference=SemanticModelReference(semantic_model_name="bridge_table"),
                entity_reference=EntityReference(element_name="account_id"),
                join_type=SemanticModelEntityJoinType(
                    left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.UNIQUE
                ),
            )
        ],
    )
    assert joinable_semantic_models["customer_other_data"] == SemanticModelLink(
        left_semantic_model_reference=SemanticModelReference(semantic_model_name="account_month_txns"),
        join_path=[
            SemanticModelEntityJoin(
                right_semantic_model_reference=SemanticModelReference(semantic_model_name="bridge_table"),
                entity_reference=EntityReference(element_name="account_id"),
                join_type=SemanticModelEntityJoinType(
                    left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.UNIQUE
                ),
            ),
            SemanticModelEntityJoin(
                right_semantic_model_reference=SemanticModelReference(semantic_model_name="customer_other_data"),
                entity_reference=EntityReference(element_name="customer_id"),
                join_type=SemanticModelEntityJoinType(
                    left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.PRIMARY
                ),
            ),
        ],
    )
    assert joinable_semantic_models["customer_table"] == SemanticModelLink(
        left_semantic_model_reference=SemanticModelReference(semantic_model_name="account_month_txns"),
        join_path=[
            SemanticModelEntityJoin(
                right_semantic_model_reference=SemanticModelReference(semantic_model_name="bridge_table"),
                entity_reference=EntityReference(element_name="account_id"),
                join_type=SemanticModelEntityJoinType(
                    left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.UNIQUE
                ),
            ),
            SemanticModelEntityJoin(
                right_semantic_model_reference=SemanticModelReference(semantic_model_name="customer_table"),
                entity_reference=EntityReference(element_name="customer_id"),
                join_type=SemanticModelEntityJoinType(
                    left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.PRIMARY
                ),
            ),
        ],
    )


def test_natural_entity_semantic_model_validation(scd_semantic_manifest_lookup: SemanticManifestLookup) -> None:
    """Tests semantic model validation for NATURAL target entity types.

    These tests rely on the scd_semantic_manifest_lookup, which makes extensive use of NATURAL key types.
    """
    natural_user_semantic_model = scd_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("primary_accounts")
    )
    primary_user_semantic_model = scd_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("users_latest")
    )
    foreign_user_semantic_model = scd_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("bookings_source")
    )
    unique_user_semantic_model = scd_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference("companies")
    )
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = SemanticModelJoinEvaluator(
        semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup
    )
    # Type refinement
    assert natural_user_semantic_model, "Could not find `primary_accounts` semantic model in scd model!"
    assert foreign_user_semantic_model, "Could not find `bookings_source` semantic model in scd model!"
    assert primary_user_semantic_model, "Could not find `users_latest` semantic model in scd model!"
    assert unique_user_semantic_model, "Could not find `companies` semantic model in scd model!"

    # Valid cases
    natural_primary = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=natural_user_semantic_model.reference,
        right_semantic_model_reference=primary_user_semantic_model.reference,
        on_entity_reference=user_entity_reference,
    )
    natural_unique = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=natural_user_semantic_model.reference,
        right_semantic_model_reference=unique_user_semantic_model.reference,
        on_entity_reference=user_entity_reference,
    )
    foreign_natural = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=foreign_user_semantic_model.reference,
        right_semantic_model_reference=natural_user_semantic_model.reference,
        on_entity_reference=user_entity_reference,
    )
    primary_natural = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=primary_user_semantic_model.reference,
        right_semantic_model_reference=natural_user_semantic_model.reference,
        on_entity_reference=user_entity_reference,
    )
    unique_natural = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=unique_user_semantic_model.reference,
        right_semantic_model_reference=natural_user_semantic_model.reference,
        on_entity_reference=user_entity_reference,
    )
    # Invalid cases
    natural_foreign = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=natural_user_semantic_model.reference,
        right_semantic_model_reference=foreign_user_semantic_model.reference,
        on_entity_reference=user_entity_reference,
    )
    natural_natural = join_evaluator.is_valid_semantic_model_join(
        left_semantic_model_reference=natural_user_semantic_model.reference,
        right_semantic_model_reference=natural_user_semantic_model.reference,
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


def test_natural_entity_instance_set_validation(
    consistent_id_object_repository: ConsistentIdObjectRepository, scd_semantic_manifest_lookup: SemanticManifestLookup
) -> None:
    """Tests instance set validation for NATURAL target entity types.

    These tests rely on the scd_semantic_manifest_lookup, which makes extensive use of NATURAL key types.
    """
    natural_user_instance_set = consistent_id_object_repository.scd_model_data_sets["primary_accounts"].instance_set
    primary_user_instance_set = consistent_id_object_repository.scd_model_data_sets["users_latest"].instance_set
    foreign_user_instance_set = consistent_id_object_repository.scd_model_data_sets["bookings_source"].instance_set
    unique_user_instance_set = consistent_id_object_repository.scd_model_data_sets["companies"].instance_set
    user_entity_reference = EntityReference(element_name="user")
    join_evaluator = SemanticModelJoinEvaluator(
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
