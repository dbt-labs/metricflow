from typing import Dict, Sequence

from metricflow.instances import DataSourceReference
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.semantics.join_validator import DataSourceIdentifierJoinType, DataSourceJoinValidator
from metricflow.object_utils import assert_values_exhausted
from metricflow.references import IdentifierReference
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository


def _get_join_types_for_identifier_type(identifier_type: IdentifierType) -> Sequence[DataSourceIdentifierJoinType]:
    """Exhaustively evaluate identifier types and return a sequence of all possible join type pairs

    The exhaustive conditional statically enforces that every identifier type is handled on the left.
    The complete set of matching join types ensures that all pairs are used.
    """

    if (
        identifier_type is IdentifierType.FOREIGN
        or identifier_type is IdentifierType.PRIMARY
        or identifier_type is IdentifierType.UNIQUE
    ):
        join_types = tuple(
            DataSourceIdentifierJoinType(left_identifier_type=identifier_type, right_identifier_type=join_type)
            for join_type in IdentifierType
        )
        return join_types
    else:
        assert_values_exhausted(identifier_type)


def test_join_type_coverage() -> None:
    """Ensures all identifier type pairs are handled somewhere in the valid/invalid join mapping sets

    This will prevent surprise RuntimeErrors in production by raising static exceptions for unhandled identifier types
    and triggering a test failure for types which are handled in a non-exhaustive fashion
    """
    all_join_types = set(
        DataSourceJoinValidator._INVALID_IDENTIFIER_JOINS + DataSourceJoinValidator._VALID_IDENTIFIER_JOINS
    )
    for identifier_type in IdentifierType:
        join_types = _get_join_types_for_identifier_type(identifier_type=identifier_type)
        for join_type in join_types:
            assert (
                join_type in all_join_types
            ), f"Unhandled identifier join type {join_type} not in valid or invalid identifier join lists!"


def __get_simple_model_user_data_source_references_by_type(
    semantic_model: SemanticModel,
) -> Dict[IdentifierType, DataSourceReference]:
    """Helper to get a set of data sources with the `user` identifier organized by identifier type"""
    foreign_user_data_source = semantic_model.data_source_semantics.get("listings_latest")
    primary_user_data_source = semantic_model.data_source_semantics.get("users_latest")
    unique_user_data_source = semantic_model.data_source_semantics.get("companies")

    assert foreign_user_data_source, "Could not find `listings_latest` data source in simple model!"
    assert primary_user_data_source, "Could not find `users_latest` data source in simple model!"
    assert unique_user_data_source, "Could not find `companies` data source in simple model!"

    return {
        IdentifierType.FOREIGN: foreign_user_data_source.reference,
        IdentifierType.PRIMARY: primary_user_data_source.reference,
        IdentifierType.UNIQUE: unique_user_data_source.reference,
    }


def test_distinct_target_data_source_join_validation(simple_semantic_model: SemanticModel) -> None:
    """Tests data source join validation to a PRIMARY or UNIQUE identifier

    PRIMARY and UNIQUE identifier targets should be valid for any join at the data source level because they both
    represent identifier columns with distinct value sets, and as such there is no risk of inadvertent fanout joins.
    """
    data_source_references = __get_simple_model_user_data_source_references_by_type(simple_semantic_model)
    user_identifier_reference = IdentifierReference(element_name="user")
    join_validator = DataSourceJoinValidator(data_source_semantics=simple_semantic_model.data_source_semantics)

    foreign_primary = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.FOREIGN],
        right_data_source_reference=data_source_references[IdentifierType.PRIMARY],
        on_identifier_reference=user_identifier_reference,
    )
    primary_primary = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.PRIMARY],
        right_data_source_reference=data_source_references[IdentifierType.PRIMARY],
        on_identifier_reference=user_identifier_reference,
    )
    unique_primary = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.UNIQUE],
        right_data_source_reference=data_source_references[IdentifierType.PRIMARY],
        on_identifier_reference=user_identifier_reference,
    )
    foreign_unique = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.FOREIGN],
        right_data_source_reference=data_source_references[IdentifierType.UNIQUE],
        on_identifier_reference=user_identifier_reference,
    )
    primary_unique = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.PRIMARY],
        right_data_source_reference=data_source_references[IdentifierType.UNIQUE],
        on_identifier_reference=user_identifier_reference,
    )
    unique_unique = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.UNIQUE],
        right_data_source_reference=data_source_references[IdentifierType.UNIQUE],
        on_identifier_reference=user_identifier_reference,
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
        f"All data source level join types for primary and unique targets should be valid, but we found "
        f"at least one that was not! Incorrectly failing types: {[k for k,v in results.items() if not v]}."
    )


def test_foreign_target_data_source_join_validation(simple_semantic_model: SemanticModel) -> None:
    """Tests data source join validation to FOREIGN identifier types

    These should all fail by default, as fanout joins are not supported
    """
    data_source_references = __get_simple_model_user_data_source_references_by_type(simple_semantic_model)
    user_identifier_reference = IdentifierReference(element_name="user")
    join_validator = DataSourceJoinValidator(data_source_semantics=simple_semantic_model.data_source_semantics)

    foreign_foreign = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.FOREIGN],
        right_data_source_reference=data_source_references[IdentifierType.FOREIGN],
        on_identifier_reference=user_identifier_reference,
    )
    primary_foreign = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.PRIMARY],
        right_data_source_reference=data_source_references[IdentifierType.FOREIGN],
        on_identifier_reference=user_identifier_reference,
    )
    unique_foreign = join_validator.is_valid_data_source_join(
        left_data_source_reference=data_source_references[IdentifierType.UNIQUE],
        right_data_source_reference=data_source_references[IdentifierType.FOREIGN],
        on_identifier_reference=user_identifier_reference,
    )

    results = {
        "foreign to foreign": foreign_foreign,
        "primary to foreign": primary_foreign,
        "unique to foreign": unique_foreign,
    }
    assert not any(results.values()), (
        f"All data source level joins against foreign targets should be invalid, but we found at least one "
        f"that was not! Incorrectly passing types: {[k for k,v in results.items() if v]}."
    )


def test_data_source_join_validation_on_missing_identifier(simple_semantic_model: SemanticModel) -> None:
    """Tests data source join validation where the identifier is missing from one or both data sources"""
    primary_listing_data_source = simple_semantic_model.data_source_semantics.get("listings_latest")
    assert primary_listing_data_source, "Could not find data source `listings_latest` in the simple model!"
    no_listing_data_source = simple_semantic_model.data_source_semantics.get("id_verifications")
    assert no_listing_data_source, "Could not find data source `id_verifications` in the simple model!"
    listing_identifier_reference = IdentifierReference(element_name="listing")
    join_validator = DataSourceJoinValidator(data_source_semantics=simple_semantic_model.data_source_semantics)

    assert not join_validator.is_valid_data_source_join(
        left_data_source_reference=no_listing_data_source.reference,
        right_data_source_reference=primary_listing_data_source.reference,
        on_identifier_reference=listing_identifier_reference,
    ), (
        "Found valid join on `listing` involving the `id_verifications` data source, which does not include the "
        "`listing` identifier!"
    )


def test_distinct_target_instance_set_join_validation(
    consistent_id_object_repository: ConsistentIdObjectRepository, simple_semantic_model: SemanticModel
) -> None:
    """Tests instance set join validation to a PRIMARY or UNIQUE identifier"""
    foreign_user_instance_set = consistent_id_object_repository.simple_model_data_sets["listings_latest"].instance_set
    primary_user_instance_set = consistent_id_object_repository.simple_model_data_sets["users_latest"].instance_set
    unique_user_instance_set = consistent_id_object_repository.simple_model_data_sets["companies"].instance_set
    user_identifier_reference = IdentifierReference(element_name="user")
    join_validator = DataSourceJoinValidator(data_source_semantics=simple_semantic_model.data_source_semantics)

    foreign_primary = join_validator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    primary_primary = join_validator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    unique_primary = join_validator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=primary_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    foreign_unique = join_validator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    primary_unique = join_validator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    unique_unique = join_validator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=unique_user_instance_set,
        on_identifier_reference=user_identifier_reference,
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
    consistent_id_object_repository: ConsistentIdObjectRepository, simple_semantic_model: SemanticModel
) -> None:
    """Tests data source join validation to FOREIGN identifier types"""
    foreign_user_instance_set = consistent_id_object_repository.simple_model_data_sets["listings_latest"].instance_set
    primary_user_instance_set = consistent_id_object_repository.simple_model_data_sets["users_latest"].instance_set
    unique_user_instance_set = consistent_id_object_repository.simple_model_data_sets["companies"].instance_set
    user_identifier_reference = IdentifierReference(element_name="user")
    join_validator = DataSourceJoinValidator(data_source_semantics=simple_semantic_model.data_source_semantics)

    foreign_foreign = join_validator.is_valid_instance_set_join(
        left_instance_set=foreign_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    primary_foreign = join_validator.is_valid_instance_set_join(
        left_instance_set=primary_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )
    unique_foreign = join_validator.is_valid_instance_set_join(
        left_instance_set=unique_user_instance_set,
        right_instance_set=foreign_user_instance_set,
        on_identifier_reference=user_identifier_reference,
    )

    results = {
        "foreign to foreign": foreign_foreign,
        "primary to foreign": primary_foreign,
        "unique to foreign": unique_foreign,
    }
    assert not any(results.values()), (
        f"All data source level joins against foreign targets should be invalid, but we found at least one "
        f"that was not! Incorrectly passing types: {[k for k,v in results.items() if v]}."
    )
