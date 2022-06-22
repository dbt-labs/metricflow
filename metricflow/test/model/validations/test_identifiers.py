import pytest
import copy
import re

from typing import Callable

from metricflow.object_utils import flatten_nested_sequence
from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.identifier import IdentifierType, Identifier, CompositeSubIdentifier
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import MetricType, MetricTypeParams
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.specs import DimensionReference, MeasureReference, IdentifierReference
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta, metric_with_guaranteed_meta
from metricflow.time.time_granularity import TimeGranularity
from metricflow.test.test_utils import find_data_source_with


def test_data_source_cant_have_more_than_one_primary_identifier(
    simple_model__pre_transforms: UserConfiguredModel,
) -> None:  # noqa: D
    """Add an additional primary identifier to a data source and assert that it cannot have two"""
    model = copy.deepcopy(simple_model__pre_transforms)
    func: Callable[[DataSource], bool] = lambda data_source: len(data_source.identifiers) > 1

    multiple_identifier_data_source, _ = find_data_source_with(model, func)

    identifier_names = set()
    for identifier in multiple_identifier_data_source.identifiers:
        identifier.type = IdentifierType.PRIMARY
        identifier_names.add(identifier.name)

    build = ModelValidator().validate_model(model)

    future_issue = (
        f"Data sources can have only one primary identifier. The data source"
        f" `{multiple_identifier_data_source.name}` has {len(identifier_names)}"
    )

    found_future_issue = False

    if build.issues is not None:
        for issue in build.issues:
            if re.search(future_issue, issue.message):
                found_future_issue = True

    assert found_future_issue


def test_invalid_composite_identifiers() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"If sub identifier has same name"):
        dim_reference = DimensionReference(element_name="time")
        measure_reference = MeasureReference(element_name="foo")
        measure2_reference = MeasureReference(element_name="metric_with_no_time_dim")
        identifier_reference = IdentifierReference(element_name="thorium")
        foreign_identifier_reference = IdentifierReference(element_name="composite_thorium")
        ModelValidator().checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_reference.element_name}, {measure_reference.element_name}, thorium_id FROM bar",
                        measures=[Measure(name=measure_reference, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_reference,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        identifiers=[
                            Identifier(name=identifier_reference, type=IdentifierType.PRIMARY, expr="thorium_id"),
                            Identifier(
                                name=foreign_identifier_reference,
                                type=IdentifierType.FOREIGN,
                                identifiers=[
                                    CompositeSubIdentifier(name=identifier_reference, expr="not_thorium_id"),
                                ],
                            ),
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure2_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference]),
                    )
                ],
                materializations=[],
            )
        )


def test_composite_identifiers_nonexistent_ref() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"Identifier ref must reference an existing identifier by name"):
        dim_reference = DimensionReference(element_name="time")
        measure_reference = MeasureReference(element_name="foo")
        measure2_reference = MeasureReference(element_name="metric_with_no_time_dim")
        identifier_reference = IdentifierReference(element_name="thorium")
        foreign_identifier_reference = IdentifierReference(element_name="composite_thorium")
        ModelValidator().checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_reference.element_name}, {measure_reference.element_name}, thorium_id FROM bar",
                        measures=[Measure(name=measure_reference, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_reference,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        identifiers=[
                            Identifier(name=identifier_reference, type=IdentifierType.PRIMARY, expr="thorium_id"),
                            Identifier(
                                name=foreign_identifier_reference,
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
                        name=measure2_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference]),
                    )
                ],
                materializations=[],
            )
        )


def test_composite_identifiers_ref_and_name() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"Both ref and name/expr set in sub identifier of identifier"):
        dim_reference = DimensionReference(element_name="time")
        measure_reference = MeasureReference(element_name="foo")
        measure2_reference = MeasureReference(element_name="metric_with_no_time_dim")
        identifier_reference = IdentifierReference(element_name="thorium")
        foreign_identifier_reference = IdentifierReference(element_name="composite_thorium")
        foreign_identifier2_reference = IdentifierReference(element_name="shouldnt_have_both")
        ModelValidator().checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_reference.element_name}, {measure_reference.element_name}, thorium_id FROM bar",
                        measures=[Measure(name=measure_reference, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_reference,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        identifiers=[
                            Identifier(name=identifier_reference, type=IdentifierType.PRIMARY, expr="thorium_id"),
                            Identifier(
                                name=foreign_identifier_reference,
                                type=IdentifierType.FOREIGN,
                                identifiers=[
                                    CompositeSubIdentifier(
                                        ref="ident_that_doesnt_exist", name=foreign_identifier2_reference
                                    ),
                                ],
                            ),
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure2_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference]),
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
        name=IdentifierReference(element_name="composite_identifier"),
        type=IdentifierType.FOREIGN,
        identifiers=[CompositeSubIdentifier(ref="sub_identifier1")],
    )
    bookings_source.identifiers = flatten_nested_sequence([bookings_source.identifiers, [identifier_bookings]])

    identifier_listings = Identifier(
        name=IdentifierReference(element_name="composite_identifier"),
        type=IdentifierType.FOREIGN,
        identifiers=[CompositeSubIdentifier(ref="sub_identifier2")],
    )
    listings_latest.identifiers = flatten_nested_sequence([listings_latest.identifiers, [identifier_listings]])

    build = ModelValidator().validate_model(model)

    expected_error_message_fragment = "does not have consistent sub-identifiers"
    error_count = len([issue for issue in build.issues if re.search(expected_error_message_fragment, issue.message)])

    assert error_count == 1
