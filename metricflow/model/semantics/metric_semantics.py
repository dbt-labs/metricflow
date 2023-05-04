import logging
from typing import Dict, List, FrozenSet, Sequence

from dbt_semantic_interfaces.objects.metric import Metric, MetricType
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import MetricReference
from metricflow.errors.errors import MetricNotFoundError, DuplicateMetricError, NonExistentMeasureError
from metricflow.model.semantics.data_source_join_evaluator import MAX_JOIN_HOPS
from metricflow.model.semantics.data_source_semantics import DataSourceSemantics
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.linkable_spec_resolver import ValidLinkableSpecResolver
from metricflow.model.spec_converters import WhereConstraintConverter
from metricflow.protocols.semantics import MetricSemanticsAccessor
from metricflow.specs import (
    MetricSpec,
    LinkableInstanceSpec,
    MetricInputMeasureSpec,
    MeasureSpec,
)

logger = logging.getLogger(__name__)


class MetricSemantics(MetricSemanticsAccessor):  # noqa: D
    def __init__(  # noqa: D
        self, user_configured_model: UserConfiguredModel, data_source_semantics: DataSourceSemantics
    ) -> None:
        self._user_configured_model = user_configured_model
        self._metrics: Dict[MetricReference, Metric] = {}
        self._data_source_semantics = data_source_semantics

        for metric in self._user_configured_model.metrics:
            self.add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            user_configured_model=self._user_configured_model,
            data_source_semantics=data_source_semantics,
            max_entity_links=MAX_JOIN_HOPS,
        )

    def element_specs_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> Sequence[LinkableInstanceSpec]:
        """Dimensions common to all metrics requested (intersection)"""

        all_linkable_specs = self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=metric_references,
            with_any_of=with_any_property,
            without_any_of=without_any_property,
        ).as_spec_set

        return sorted(all_linkable_specs.as_tuple, key=lambda x: x.qualified_name)

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
        """Add metric, validating presence of required measures"""
        metric_reference = MetricReference(element_name=metric.name)
        if metric_reference in self._metrics:
            raise DuplicateMetricError(f"Metric `{metric.name}` has already been registered")
        for measure_reference in metric.measure_references:
            if measure_reference not in self._data_source_semantics.measure_references:
                raise NonExistentMeasureError(
                    f"Metric `{metric.name}` references measure `{measure_reference}` which has not been registered"
                )
        self._metrics[metric_reference] = metric

    def measures_for_metric(self, metric_reference: MetricReference) -> Sequence[MetricInputMeasureSpec]:
        """Return the measure specs required to compute the metric."""
        metric = self.get_metric(metric_reference)
        input_measure_specs: List[MetricInputMeasureSpec] = []

        for input_measure in metric.input_measures:
            spec_constraint = (
                WhereConstraintConverter.convert_to_spec_where_constraint(
                    data_source_semantics=self._data_source_semantics,
                    where_constraint=input_measure.constraint,
                )
                if input_measure.constraint is not None
                else None
            )
            measure_spec = MeasureSpec(
                element_name=input_measure.name,
                non_additive_dimension_spec=self._data_source_semantics.non_additive_dimension_specs_by_measure.get(
                    input_measure.measure_reference
                ),
            )
            spec = MetricInputMeasureSpec(
                measure_spec=measure_spec,
                constraint=spec_constraint,
                alias=input_measure.alias,
            )
            input_measure_specs.append(spec)

        return tuple(input_measure_specs)

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

    def metric_input_specs_for_metric(self, metric_reference: MetricReference) -> Sequence[MetricSpec]:
        """Return the metric specs referenced by the metric. Current use case is for derived metrics."""
        metric = self.get_metric(metric_reference)
        input_metric_specs: List[MetricSpec] = []

        for input_metric in metric.input_metrics:
            original_metric_obj = self.get_metric(input_metric.as_reference)

            # This is the constraint parameter added to the input metric in the derived metric definition
            combined_spec_constraint = (
                WhereConstraintConverter.convert_to_spec_where_constraint(
                    data_source_semantics=self._data_source_semantics,
                    where_constraint=input_metric.constraint,
                )
                if input_metric.constraint is not None
                else None
            )

            # This is the constraint parameter included in the original input metric definition
            if original_metric_obj.constraint:
                original_metric_constraint = WhereConstraintConverter.convert_to_spec_where_constraint(
                    data_source_semantics=self._data_source_semantics,
                    where_constraint=original_metric_obj.constraint,
                )
                combined_spec_constraint = (
                    combined_spec_constraint.combine(original_metric_constraint)
                    if combined_spec_constraint
                    else original_metric_constraint
                )

            spec = MetricSpec(
                element_name=input_metric.name,
                constraint=combined_spec_constraint,
                alias=input_metric.alias,
                offset_window=input_metric.offset_window,
                offset_to_grain=input_metric.offset_to_grain,
            )
            input_metric_specs.append(spec)
        return tuple(input_metric_specs)
