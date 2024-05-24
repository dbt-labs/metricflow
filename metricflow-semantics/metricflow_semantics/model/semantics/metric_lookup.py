from __future__ import annotations

import functools
import logging
import time
from typing import Dict, FrozenSet, Optional, Sequence, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.metric import Metric, MetricInputMeasure, MetricType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference

from metricflow_semantics.errors.error_classes import DuplicateMetricError, MetricNotFoundError, NonExistentMeasureError
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
)
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.spec_classes import TimeDimensionSpec

logger = logging.getLogger(__name__)


class MetricLookup:
    """Tracks semantic information for metrics by linking them to semantic models."""

    def __init__(self, semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup) -> None:
        """Initializer.

        Args:
            semantic_manifest: used to fetch and load the metrics and initialize the linkable spec resolver
            semantic_model_lookup: provides access to semantic model metadata for various lookup operations
        """
        self._metrics: Dict[MetricReference, Metric] = {}
        self._semantic_model_lookup = semantic_model_lookup

        for metric in semantic_manifest.metrics:
            self._add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            semantic_manifest=semantic_manifest,
            semantic_model_lookup=semantic_model_lookup,
            max_entity_links=MAX_JOIN_HOPS,
        )

    @functools.lru_cache
    def linkable_elements_for_measure(
        self,
        measure_reference: MeasureReference,
        with_any_of: Optional[FrozenSet[LinkableElementProperty]] = None,
        without_any_of: Optional[FrozenSet[LinkableElementProperty]] = None,
    ) -> LinkableElementSet:
        """Return the set of linkable elements reachable from a given measure."""
        frozen_with_any_of = LinkableElementProperty.all_properties() if with_any_of is None else with_any_of
        frozen_without_any_of = frozenset() if without_any_of is None else without_any_of

        start_time = time.time()
        linkable_element_set = self._linkable_spec_resolver.get_linkable_element_set_for_measure(
            measure_reference=measure_reference,
            with_any_of=frozen_with_any_of,
            without_any_of=frozen_without_any_of,
        )
        logger.info(
            f"Getting valid linkable elements for measure '{measure_reference.element_name}' took: {time.time() - start_time:.2f}s"
        )

        return linkable_element_set

    @functools.lru_cache
    def linkable_elements_for_no_metrics_query(
        self,
        with_any_of: Optional[FrozenSet[LinkableElementProperty]] = None,
        without_any_of: Optional[FrozenSet[LinkableElementProperty]] = None,
    ) -> LinkableElementSet:
        """Return the reachable linkable elements for a dimension values query with no metrics."""
        frozen_with_any_of = LinkableElementProperty.all_properties() if with_any_of is None else with_any_of
        frozen_without_any_of = frozenset() if without_any_of is None else without_any_of

        return self._linkable_spec_resolver.get_linkable_elements_for_distinct_values_query(
            with_any_of=frozen_with_any_of,
            without_any_of=frozen_without_any_of,
        )

    @functools.lru_cache
    def linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperty] = LinkableElementProperty.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperty] = frozenset(),
    ) -> LinkableElementSet:
        """Retrieve the matching set of linkable elements common to all metrics requested (intersection)."""
        return self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=metric_references,
            with_any_of=with_any_property,
            without_any_of=without_any_property,
        )

    def get_metrics(self, metric_references: Sequence[MetricReference]) -> Sequence[Metric]:  # noqa: D102
        res = []
        for metric_reference in metric_references:
            if metric_reference not in self._metrics:
                raise MetricNotFoundError(
                    f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered"
                )
            res.append(self._metrics[metric_reference])

        return res

    @property
    def metric_references(self) -> Sequence[MetricReference]:  # noqa: D102
        return list(self._metrics.keys())

    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa: D102
        if metric_reference not in self._metrics:
            raise MetricNotFoundError(f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered")
        return self._metrics[metric_reference]

    def _add_metric(self, metric: Metric) -> None:
        """Add metric, validating presence of required measures."""
        metric_reference = MetricReference(element_name=metric.name)
        if metric_reference in self._metrics:
            raise DuplicateMetricError(f"Metric `{metric.name}` has already been registered")
        for measure_reference in metric.measure_references:
            if measure_reference not in self._semantic_model_lookup.measure_references:
                raise NonExistentMeasureError(
                    f"Metric `{metric.name}` references measure `{measure_reference}` which has not been registered"
                )
        self._metrics[metric_reference] = metric

    def configured_input_measure_for_metric(  # noqa: D102
        self, metric_reference: MetricReference
    ) -> Optional[MetricInputMeasure]:  # noqa: D102
        metric = self.get_metric(metric_reference=metric_reference)
        if metric.type is MetricType.CUMULATIVE or metric.type is MetricType.SIMPLE:
            assert len(metric.input_measures) == 1, "Simple and cumulative metrics should have one input measure."
            return metric.input_measures[0]
        elif (
            metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED or metric.type is MetricType.CONVERSION
        ):
            return None
        else:
            assert_values_exhausted(metric.type)

    def contains_cumulative_or_time_offset_metric(self, metric_references: Sequence[MetricReference]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric or a derived metric with time offset."""
        for metric_reference in metric_references:
            metric = self.get_metric(metric_reference)
            if metric.type == MetricType.CUMULATIVE:
                return True
            elif metric.type == MetricType.DERIVED:
                for input_metric in metric.type_params.metrics or []:
                    if input_metric.offset_window or input_metric.offset_to_grain:
                        return True
        return False

    def _get_agg_time_dimension_specs_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        """Retrieves the aggregate time dimensions associated with the metric's measures."""
        metric = self.get_metric(metric_reference)
        specs: Set[TimeDimensionSpec] = set()
        for input_measure in metric.input_measures:
            time_dimension_specs = self._semantic_model_lookup.get_agg_time_dimension_specs_for_measure(
                measure_reference=input_measure.measure_reference
            )
            specs.update(time_dimension_specs)
        return list(specs)

    def get_valid_agg_time_dimensions_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        """Get the agg time dimension specs that can be used in place of metric time for this metric, if applicable."""
        agg_time_dimension_specs = self._get_agg_time_dimension_specs_for_metric(metric_reference)
        distinct_agg_time_dimension_identifiers = set(
            [(spec.reference, spec.entity_links) for spec in agg_time_dimension_specs]
        )
        if len(distinct_agg_time_dimension_identifiers) != 1:
            # If the metric's input measures have different agg_time_dimensions, user must use metric_time.
            return []

        agg_time_dimension_reference, agg_time_dimension_entity_links = distinct_agg_time_dimension_identifiers.pop()
        valid_agg_time_dimension_specs = TimeDimensionSpec.generate_possible_specs_for_time_dimension(
            time_dimension_reference=agg_time_dimension_reference,
            entity_links=agg_time_dimension_entity_links,
        )
        return valid_agg_time_dimension_specs
