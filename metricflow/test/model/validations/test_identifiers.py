import copy
import re
from typing import Callable

import pytest

from metricflow.aggregation_properties import AggregationType
from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.identifier import IdentifierType, Identifier, CompositeSubIdentifier
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.metric import MetricType, MetricTypeParams
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.object_utils import flatten_nested_sequence
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta, metric_with_guaranteed_meta
from metricflow.test.test_utils import find_data_source_with
from metricflow.time.time_granularity import TimeGranularity


def test_data_source_cant_have_more_than_one_primary_identifier(
    simple_model__pre_transforms: UserConfiguredModel,
) -> None:  # noqa: D
    """Add an additional primary identifier to a data source and assert that it cannot have two"""
    model = copy.deepcopy(simple_model__pre_transforms)
    func: Callable[[DataSource], bool] = lambda data_source: len(data_source.identifiers) > 1

    multiple_identifier_data_source, _ = find_data_source_with(model, func)

    identifier_references = set()
    for identifier in multiple_identifier_data_source.identifiers:
        identifier.type = IdentifierType.PRIMARY
        identifier_references.add(identifier.reference)

    build = ModelValidator().validate_model(model)

    future_issue = (
        f"Data sources can have only one primary identifier. The data source"
        f" `{multiple_identifier_data_source.name}` has {len(identifier_references)}"
    )

    found_future_issue = False

    if build.issues is not None:
        for issue in build.issues.all_issues:
            if re.search(future_issue, issue.message):
                found_future_issue = True

    assert found_future_issue


def test_invalid_composite_identifiers() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"If sub identifier has same name"):
        dim_name = "time"
        measure_name = "foo"
        measure2_name = "metric_with_no_time_dim"
        identifier_name = "thorium"
        foreign_identifier_name = "composite_thorium"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_name}, {measure_name}, thorium_id FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        identifiers=[
                            Identifier(name=identifier_name, type=IdentifierType.PRIMARY, expr="thorium_id"),
                            Identifier(
                                name=foreign_identifier_name,
                                type=IdentifierType.FOREIGN,
                                identifiers=[
                                    CompositeSubIdentifier(name=identifier_name, expr="not_thorium_id"),
                                ],
                            ),
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure2_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
                materializations=[],
            )
        )


def test_composite_identifiers_nonexistent_ref() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"Identifier ref must reference an existing identifier by name"):
        dim_name = "time"
        measure_name = "foo"
        measure2_name = "metric_with_no_time_dim"
        identifier_name = "thorium"
        foreign_identifier_name = "composite_thorium"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_name}, {measure_name}, thorium_id FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        identifiers=[
                            Identifier(name=identifier_name, type=IdentifierType.PRIMARY, expr="thorium_id"),
                            Identifier(
                                name=foreign_identifier_name,
                                type=IdentifierType.FOREIGN,
                                identifiers=[
                                    CompositeSubIdentifier(ref="ident_that_doesnt_exist"),
                                ],
                            ),
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure2_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
                materializations=[],
            )
        )


def test_composite_identifiers_ref_and_name() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"Both ref and name/expr set in sub identifier of identifier"):
        dim_name = "time"
        measure_name = "foo"
        measure2_name = "metric_with_no_time_dim"
        identifier_name = "thorium"
        foreign_identifier_name = "composite_thorium"
        foreign_identifier2_name = "shouldnt_have_both"
        model_validator = ModelValidator()
        model_validator.checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_name}, {measure_name}, thorium_id FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        identifiers=[
                            Identifier(name=identifier_name, type=IdentifierType.PRIMARY, expr="thorium_id"),
                            Identifier(
                                name=foreign_identifier_name,
                                type=IdentifierType.FOREIGN,
                                identifiers=[
                                    CompositeSubIdentifier(
                                        ref="ident_that_doesnt_exist", name=foreign_identifier2_name
                                    ),
                                ],
                            ),
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure2_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
                materializations=[],
            )
        )


def test_mismatched_identifier(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    """Testing two mismatched identifiers in two data sources

    Add two identifiers with mismatched sub-identifiers to two data sources in the model
    Ensure that our composite identifiers rule catches this incompatibility
    """
    model = copy.deepcopy(simple_model__pre_transforms)

    bookings_source, _ = find_data_source_with(
        model=model,
        function=lambda data_source: data_source.name == "bookings_source",
    )
    listings_latest, _ = find_data_source_with(
        model=model,
        function=lambda data_source: data_source.name == "listings_latest",
    )

    identifier_bookings = Identifier(
        name="composite_identifier",
        type=IdentifierType.FOREIGN,
        identifiers=[CompositeSubIdentifier(ref="sub_identifier1")],
    )
    bookings_source.identifiers = flatten_nested_sequence([bookings_source.identifiers, [identifier_bookings]])

    identifier_listings = Identifier(
        name="composite_identifier",
        type=IdentifierType.FOREIGN,
        identifiers=[CompositeSubIdentifier(ref="sub_identifier2")],
    )
    listings_latest.identifiers = flatten_nested_sequence([listings_latest.identifiers, [identifier_listings]])

    build = ModelValidator().validate_model(model)

    expected_error_message_fragment = "does not have consistent sub-identifiers"
    error_count = len(
        [issue for issue in build.issues.all_issues if re.search(expected_error_message_fragment, issue.message)]
    )

    assert error_count == 1
