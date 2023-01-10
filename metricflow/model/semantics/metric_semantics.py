import logging

from typing import Dict, List, FrozenSet, Set, Tuple, Sequence

from metricflow.errors.errors import MetricNotFoundError, DuplicateMetricError, NonExistentMeasureError
from metricflow.model.objects.metric import Metric, MetricType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.data_source_semantics import DataSourceSemantics
from metricflow.model.semantics.linkable_spec_resolver import ValidLinkableSpecResolver
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.spec_converters import WhereConstraintConverter
from metricflow.references import MetricReference
from metricflow.specs import MetricSpec, LinkableInstanceSpec, MetricInputMeasureSpec, MeasureSpec
from metricflow.model.semantics.data_source_join_evaluator import MAX_JOIN_HOPS


logger = logging.getLogger(__name__)


class MetricSemantics:  # noqa: D
    def __init__(  # noqa: D
        self, user_configured_model: UserConfiguredModel, data_source_semantics: DataSourceSemantics
    ) -> None:
        self._user_configured_model = user_configured_model
        self._metrics: Dict[MetricReference, Metric] = {}
        self._data_source_semantics = data_source_semantics

        # Dict from the name of the metric to the hash.
        self._metric_hashes: Dict[MetricReference, str] = {}

        for metric in self._user_configured_model.metrics:
            self.add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            user_configured_model=self._user_configured_model,
            data_source_semantics=data_source_semantics,
            max_identifier_links=MAX_JOIN_HOPS,
        )

    def element_specs_for_metrics(
        self,
        metric_references: List[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> List[LinkableInstanceSpec]:
        """Dimensions common to all metrics requested (intersection)"""

        all_linkable_specs = self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=metric_references,
            with_any_of=with_any_property,
            without_any_of=without_any_property,
        ).as_spec_set

        return sorted(all_linkable_specs.as_tuple, key=lambda x: x.qualified_name)

    def get_metrics(self, metric_references: List[MetricReference]) -> List[Metric]:  # noqa: D
        res = []
        for metric_reference in metric_references:
            if metric_reference not in self._metrics:
                raise MetricNotFoundError(
                    f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered"
                )
            res.append(self._metrics[metric_reference])

        return res

    @property
    def metric_references(self) -> List[MetricReference]:  # noqa: D
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
        self._metric_hashes[metric_reference] = metric.definition_hash

    @property
    def valid_hashes(self) -> Set[str]:
        """Return all of the hashes of the metric definitions."""
        return set(self._metric_hashes.values())

    def measures_for_metric(self, metric_reference: MetricReference) -> Tuple[MetricInputMeasureSpec, ...]:
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

    def metric_input_specs_for_metric(self, metric_reference: MetricReference) -> Tuple[MetricSpec, ...]:
        """Return the metric specs referenced by the metric. Current use case is for derived metrics."""
        metric = self.get_metric(metric_reference)
        input_metric_specs: List[MetricSpec] = []

        for input_metric in metric.input_metrics:
            spec_constraint = (
                WhereConstraintConverter.convert_to_spec_where_constraint(
                    data_source_semantics=self._data_source_semantics,
                    where_constraint=input_metric.constraint,
                )
                if input_metric.constraint is not None
                else None
            )
            spec = MetricSpec(
                element_name=input_metric.name,
                constraint=spec_constraint,
                alias=input_metric.alias,
                offset_window=input_metric.offset_window,
                offset_to_grain=input_metric.offset_to_grain,
            )
            input_metric_specs.append(spec)
        return tuple(input_metric_specs)
