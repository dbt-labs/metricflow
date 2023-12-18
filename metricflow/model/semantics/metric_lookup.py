from __future__ import annotations

import logging
from typing import Dict, FrozenSet, Optional, Sequence, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.metric import Metric, MetricInputMeasure, MetricType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference

from metricflow.errors.errors import DuplicateMetricError, MetricNotFoundError, NonExistentMeasureError
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.linkable_spec_resolver import LinkableElementSet, ValidLinkableSpecResolver
from metricflow.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow.protocols.semantics import MetricAccessor
from metricflow.specs.specs import LinkableInstanceSpec

logger = logging.getLogger(__name__)


class MetricLookup(MetricAccessor):  # noqa: D
    def __init__(  # noqa: D
        self, semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup
    ) -> None:
        self._semantic_manifest = semantic_manifest
        self._metrics: Dict[MetricReference, Metric] = {}
        self._semantic_model_lookup = semantic_model_lookup

        for metric in self._semantic_manifest.metrics:
            self.add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            semantic_manifest=self._semantic_manifest,
            semantic_model_lookup=semantic_model_lookup,
            max_entity_links=MAX_JOIN_HOPS,
        )

    def element_specs_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> Sequence[LinkableInstanceSpec]:
        """Dimensions common to all metrics requested (intersection)."""
        all_linkable_specs = self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=metric_references,
            with_any_of=with_any_property,
            without_any_of=without_any_property,
        ).as_spec_set

        return sorted(all_linkable_specs.as_tuple, key=lambda x: x.qualified_name)

    def group_by_item_specs_for_measure(
        self,
        measure_reference: MeasureReference,
        with_any_of: Optional[Set[LinkableElementProperties]] = None,
        without_any_of: Optional[Set[LinkableElementProperties]] = None,
    ) -> Sequence[LinkableInstanceSpec]:
        """Return group-by-items that are possible for a measure."""
        frozen_with_any_of = (
            LinkableElementProperties.all_properties() if with_any_of is None else frozenset(with_any_of)
        )
        frozen_without_any_of = frozenset() if without_any_of is None else frozenset(without_any_of)

        return self._linkable_spec_resolver.get_linkable_element_set_for_measure(
            measure_reference=measure_reference,
            with_any_of=frozen_with_any_of,
            without_any_of=frozen_without_any_of,
        ).as_spec_set.as_tuple

    def group_by_item_specs_for_no_metrics_query(
        self,
        with_any_of: Optional[Set[LinkableElementProperties]] = None,
        without_any_of: Optional[Set[LinkableElementProperties]] = None,
    ) -> Sequence[LinkableInstanceSpec]:
        """Return the possible group-by-items for a dimension values query with no metrics."""
        frozen_with_any_of = (
            LinkableElementProperties.all_properties() if with_any_of is None else frozenset(with_any_of)
        )
        frozen_without_any_of = frozenset() if without_any_of is None else frozenset(without_any_of)

        all_linkable_specs = self._linkable_spec_resolver.get_linkable_elements_for_distinct_values_query(
            with_any_of=frozen_with_any_of,
            without_any_of=frozen_without_any_of,
        ).as_spec_set

        return sorted(all_linkable_specs.as_tuple, key=lambda x: x.qualified_name)

    def linkable_set_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> LinkableElementSet:
        """Similar to element_specs_for_metrics(), but as a set with more context."""
        return self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=metric_references,
            with_any_of=with_any_property,
            without_any_of=without_any_property,
        )

    def get_metrics(self, metric_references: Sequence[MetricReference]) -> Sequence[Metric]:  # noqa: D
        res = []
        for metric_reference in metric_references:
            if metric_reference not in self._metrics:
                raise MetricNotFoundError(
                    f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered"
                )
            res.append(self._metrics[metric_reference])

        return res

    @property
    def metric_references(self) -> Sequence[MetricReference]:  # noqa: D
        return list(self._metrics.keys())

    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa:D
        if metric_reference not in self._metrics:
            raise MetricNotFoundError(f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered")
        return self._metrics[metric_reference]

    def add_metric(self, metric: Metric) -> None:
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

    def configured_input_measure_for_metric(  # noqa: D
        self, metric_reference: MetricReference
    ) -> Optional[MetricInputMeasure]:
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
