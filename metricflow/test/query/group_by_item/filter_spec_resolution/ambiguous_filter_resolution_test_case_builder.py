from __future__ import annotations

import copy
import itertools
import logging
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.test.model.modify.modify_input_measure_filter import ModifyInputMeasureFilterTransform
from metricflow.test.model.modify.modify_input_metric_filter import ModifyInputMetricFilterTransform
from metricflow.test.model.modify.modify_metric_filter import ModifyMetricFilterTransform

logger = logging.getLogger(__name__)


class AmbiguousFilterResolutionTestCaseBuilder:
    """This aims to produce a set of cases for testing (ambiguous) resolution of group-by-items in filters.

    Many test cases are possible since:

    * A filter can be located in a query, in a metric, in an input measure, or an input metric.
    * A filter can have an ambiguous time grain, or a specific time grain.
    * The filter can be defined in a location where the parent objects have a same / different base grain.
    * The ambiguity can or cannot be resolved.

    A semantic manifest and query is dynamically generated, and used by different tests.
    """

    def __init__(self, ambiguous_resolution_manifest: PydanticSemanticManifest) -> None:  # noqa: D
        self._ambiguous_resolution_manifest = ambiguous_resolution_manifest

    def _add_filter_to_input_measures(
        self, metric_reference: MetricReference, filter_sql: str
    ) -> PydanticSemanticManifest:
        ambiguous_resolution_manifest_copy = copy.deepcopy(self._ambiguous_resolution_manifest)
        ModifyInputMeasureFilterTransform(
            metric_reference=metric_reference,
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[PydanticWhereFilter(where_sql_template=filter_sql)]
            ),
        ).transform_model(
            semantic_manifest=ambiguous_resolution_manifest_copy,
        )
        return ambiguous_resolution_manifest_copy

    def _add_filter_to_input_metrics(
        self, metric_reference: MetricReference, filter_sql: str
    ) -> PydanticSemanticManifest:
        ambiguous_resolution_manifest_copy = copy.deepcopy(self._ambiguous_resolution_manifest)
        ModifyInputMetricFilterTransform(
            metric_reference=metric_reference,
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[PydanticWhereFilter(where_sql_template=filter_sql)]
            ),
        ).transform_model(
            semantic_manifest=ambiguous_resolution_manifest_copy,
        )
        return ambiguous_resolution_manifest_copy

    def _add_filter_to_metric(self, metric_reference: MetricReference, filter_sql: str) -> PydanticSemanticManifest:
        ambiguous_resolution_manifest_copy = copy.deepcopy(self._ambiguous_resolution_manifest)
        ModifyMetricFilterTransform(
            metric_reference=metric_reference,
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[PydanticWhereFilter(where_sql_template=filter_sql)]
            ),
        ).transform_model(
            semantic_manifest=ambiguous_resolution_manifest_copy,
        )
        return ambiguous_resolution_manifest_copy

    def _build_one_test_configuration(
        self,
        filter_ambiguity_case: AmbiguousFilterResolutionTestConfiguration,
    ) -> AmbiguousFilterResolutionTestCase:
        """Given the test case configuration, build the test case."""
        semantic_manifest: Optional[PydanticSemanticManifest] = None

        # These are the possible SQL templates for the where filter.
        ambiguous_filter_sql = "{{ TimeDimension(" + repr(METRIC_TIME_ELEMENT_NAME) + ") }} > '2020-01-01'"
        year_filter_sql = (
            "{{"
            + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.YEAR.value)})"
            + "}} > '2020-01-01'"
        )
        day_filter_sql = (
            "{{"
            + f"TimeDimension({repr(METRIC_TIME_ELEMENT_NAME)}, {repr(TimeGranularity.DAY.value)})"
            + "}} > '2020-01-01'"
        )

        # Depending on the test case configuration, select the appropriate SQL template.
        filter_sql_candidates = set()
        if filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.AMBIGUOUS:
            filter_sql_candidates.add(ambiguous_filter_sql)
        elif filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.SPECIFIC:
            filter_sql_candidates.add(day_filter_sql)
            filter_sql_candidates.add(year_filter_sql)
        else:
            assert_values_exhausted(filter_ambiguity_case.filter_ambiguity)

        if filter_ambiguity_case.filter_validity is FilterValidity.VALID:
            filter_sql_candidates.difference_update({day_filter_sql})
        elif filter_ambiguity_case.filter_validity is FilterValidity.INVALID:
            filter_sql_candidates.difference_update({year_filter_sql})
        else:
            assert_values_exhausted(filter_ambiguity_case.filter_validity)

        assert (
            len(filter_sql_candidates) == 1
        ), f"Could not resolve to a single filter for {filter_ambiguity_case}. Got: {filter_sql_candidates}"
        filter_sql = list(filter_sql_candidates)[0]

        # Based on the test case configuration, generate the appropriate semantic manifest, query, and query filter.
        query: Tuple[MetricReference, ...] = ()
        query_filter_sql: Optional[str] = None
        if filter_ambiguity_case.filter_location is FilterLocation.FILTER_IN_QUERY_FOR_SIMPLE_METRICS:
            query_filter_sql = filter_sql
            semantic_manifest = self._ambiguous_resolution_manifest
            if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_BASE_GRAIN:
                query = (MetricReference("monthly_metric_0"), MetricReference("monthly_metric_1"))
            elif (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
            ):
                query = (MetricReference("monthly_metric_0"), MetricReference("yearly_metric_0"))
            else:
                assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
        elif filter_ambiguity_case.filter_location is FilterLocation.FILTER_IN_INPUT_MEASURE:
            metric_reference = MetricReference("monthly_metric_0")
            query = (metric_reference,)
            if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_BASE_GRAIN:
                semantic_manifest = self._add_filter_to_input_measures(metric_reference, filter_sql)
            elif (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
            ):
                raise ValueError(
                    "An input measure only applies to simple metrics, and a simple metric only has a single measure as "
                    "a parent, so it can't be of different grains."
                )
            else:
                assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
        elif filter_ambiguity_case.filter_location is FilterLocation.FILTER_IN_INPUT_METRIC:
            if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_BASE_GRAIN:
                metric_reference = MetricReference("derived_metric_with_same_parent_time_grains")
                query = (metric_reference,)
                semantic_manifest = self._add_filter_to_input_metrics(metric_reference, filter_sql)
            elif (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
            ):
                raise NotImplementedError
            else:
                assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
        elif filter_ambiguity_case.filter_location is FilterLocation.FILTER_IN_SIMPLE_METRIC:
            if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_BASE_GRAIN:
                metric_reference = MetricReference("monthly_metric_0")
                query = (metric_reference,)
                semantic_manifest = self._add_filter_to_metric(metric_reference, filter_sql)
            elif (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
            ):
                raise ValueError(
                    "A simple metric has a single measure as a parent, so it can't be of different grains."
                )
            else:
                assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)

        elif filter_ambiguity_case.filter_location is FilterLocation.FILTER_IN_DERIVED_METRIC:
            if filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_BASE_GRAIN:
                metric_reference = MetricReference("derived_metric_with_same_parent_time_grains")
                query = (metric_reference,)
                semantic_manifest = self._add_filter_to_metric(metric_reference, filter_sql)
            elif (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
            ):
                metric_reference = MetricReference("derived_metric_with_different_parent_time_grains")
                query = (metric_reference,)
                semantic_manifest = self._add_filter_to_metric(metric_reference, filter_sql)
            else:
                assert_values_exhausted(filter_ambiguity_case.parent_time_grain_congruence)
        else:
            assert_values_exhausted(filter_ambiguity_case.filter_location)

        assert semantic_manifest is not None

        return AmbiguousFilterResolutionTestCase(
            filter_ambiguity_case=filter_ambiguity_case,
            semantic_manifest=semantic_manifest,
            metrics_to_query=query,
            query_filter=PydanticWhereFilterIntersection(
                where_filters=[PydanticWhereFilter(where_sql_template=query_filter_sql)]
                if query_filter_sql is not None
                else []
            ),
        )

    def build_all_test_configurations(self) -> Sequence[AmbiguousFilterResolutionTestCase]:
        """Return all test configurations that should be used as test cases for ambiguous filter resolution."""
        parent_time_grain_congruence_values = tuple(FilterParentTimeGrainCongruence)
        filter_locations = tuple(FilterLocation)
        filter_ambiguity_values = tuple(FilterAmbiguity)
        filter_validity_values = tuple(FilterValidity)

        filter_ambiguity_cases = []
        for ambiguity_case_args in itertools.product(
            parent_time_grain_congruence_values,
            filter_locations,
            filter_ambiguity_values,
            filter_validity_values,
        ):
            filter_ambiguity_cases.append(
                AmbiguousFilterResolutionTestConfiguration(
                    parent_time_grain_congruence=ambiguity_case_args[0],
                    filter_location=ambiguity_case_args[1],
                    filter_ambiguity=ambiguity_case_args[2],
                    filter_validity=ambiguity_case_args[3],
                )
            )

        ambiguous_filter_query_cases: List[AmbiguousFilterResolutionTestCase] = []

        for filter_ambiguity_case in filter_ambiguity_cases:
            # Simple metrics only have a single measure as a parent, so it can't have parents of different grains.
            if (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
                and filter_ambiguity_case.filter_location
                in (FilterLocation.FILTER_IN_INPUT_MEASURE, FilterLocation.FILTER_IN_SIMPLE_METRIC)
            ):
                continue

            # If the parents are different grains, then an ambiguous filter can never be valid.
            if (
                filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
                and filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.AMBIGUOUS
                and filter_ambiguity_case.filter_validity is FilterValidity.VALID
            ):
                continue
            # If the parents are same grains, then an ambiguous filter is always valid.
            if (
                filter_ambiguity_case.parent_time_grain_congruence is FilterParentTimeGrainCongruence.SAME_BASE_GRAIN
                and filter_ambiguity_case.filter_ambiguity is FilterAmbiguity.AMBIGUOUS
                and filter_ambiguity_case.filter_validity is FilterValidity.INVALID
            ):
                continue

            # This case has not yet been implemented.
            if (
                filter_ambiguity_case.filter_location.FILTER_IN_INPUT_METRIC
                and filter_ambiguity_case.parent_time_grain_congruence
                is FilterParentTimeGrainCongruence.DIFFERENT_BASE_GRAIN
            ):
                continue
            ambiguous_filter_query_cases.append(
                self._build_one_test_configuration(
                    filter_ambiguity_case=filter_ambiguity_case,
                )
            )

        return ambiguous_filter_query_cases


class FilterLocation(Enum):
    """Describes the location of the filter that contains metric_time in the test setup."""

    FILTER_IN_QUERY_FOR_SIMPLE_METRICS = auto()
    FILTER_IN_SIMPLE_METRIC = auto()
    FILTER_IN_DERIVED_METRIC = auto()
    FILTER_IN_INPUT_MEASURE = auto()
    FILTER_IN_INPUT_METRIC = auto()


class FilterParentTimeGrainCongruence(Enum):
    """Specifies how the parent objects containing the filter define the base grain for a given setup.

    For example, if the where filter is for a derived metric, then a setup with the same base grain would have the
    derived metric be based on two metrics with the same time grain (e.g. two monthly metrics). For a
    different base grain, the derived metric would be based on two metrics with different time grains (e.g. a monthly
    metric and an yearly metric).
    """

    SAME_BASE_GRAIN = auto()
    DIFFERENT_BASE_GRAIN = auto()


class FilterAmbiguity(Enum):
    """Specifies whether the metric_time group-by-item in the filter is specific is ambiguous or specific.

    e.g. "TimeDimension('metric_time') = '2020-01-01'" or "TimeDimension('metric_time', 'month') = '2020-01-01'"
    """

    SPECIFIC = auto()
    AMBIGUOUS = auto()


class FilterValidity(Enum):
    """Specifies whether the metric_time group-by-item in the filter can be resolved.

    e.g. if the filter is located in a derived metric, the parents are of different grains, and the filter has an
    ambiguous metric_time, then it would resolve to be invalid since we only allow ambiguous resolution when the parent
    grains are the same.
    """

    VALID = auto()
    INVALID = auto()


@dataclass(frozen=True)
class AmbiguousFilterResolutionTestConfiguration:
    """Describes the configuration for testing resolution of ambiguous filters."""

    filter_location: FilterLocation
    parent_time_grain_congruence: FilterParentTimeGrainCongruence
    filter_ambiguity: FilterAmbiguity
    filter_validity: FilterValidity


@dataclass(frozen=True)
class AmbiguousFilterResolutionTestCase:
    """A test case that can be used to run queries to test resolution of ambiguous filters."""

    filter_ambiguity_case: AmbiguousFilterResolutionTestConfiguration
    semantic_manifest: PydanticSemanticManifest
    metrics_to_query: Tuple[MetricReference, ...]
    query_filter: WhereFilterIntersection
