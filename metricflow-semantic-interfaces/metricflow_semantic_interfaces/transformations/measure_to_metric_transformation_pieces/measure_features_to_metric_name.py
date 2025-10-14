from __future__ import annotations

from typing import Dict, Optional, Set, Tuple

from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.type_enums import MetricType


class MeasureFeaturesToMetricNameMapper:
    """Maps measure configurations to metric names, and helps add new metrics to the manifest."""

    # Until we're at minimum Python version 3.12, we can't use "type" statements, so
    # we use this for backward compatibility.
    _MetricNameKey = Tuple[str, Optional[int], bool]
    _metric_name_dict: Dict[_MetricNameKey, str]

    def __init__(self) -> None:  # noqa: D107
        self._metric_name_dict = {}

    def _get_stored_metric_name(
        self,
        measure_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
    ) -> Optional[str]:
        """Get the name of the metric that is stored for the tuple(measure, <settings>).

        where settings is a set of features that are moved from a measure_input
        object to the metric object.  It contains:
        - fill_nulls_with
        - join_to_timespine

        returns the name of the metric that is stored for this measure configuration, or
                None if no metric is stored for the measure configuration
        """
        key = (measure_name, fill_nulls_with, join_to_timespine)
        return self._metric_name_dict.get(key)

    def _store_metric_name(
        self,
        measure_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
        metric_name: str,
    ) -> None:
        """Store the name of the metric that is stored for the tuple(measure, <settings>)."""
        key = (measure_name, fill_nulls_with, join_to_timespine)
        self._metric_name_dict[key] = metric_name

    def _find_simple_metric_functional_clone_in_manifest(
        self,
        metric: PydanticMetric,
        manifest: PydanticSemanticManifest,
    ) -> Optional[PydanticMetric]:
        """Check if a metric exists in the manifest that matches the metric (except for name).

        returns the metric if it exists, otherwise None

        Note: this is appropriate for SIMPLE metrics that would **replace a measure** in
        the new YAML.  This code would require updates and expansion to handle anything beyond that.
        """

        def _metrics_equivalent(search_metric: PydanticMetric, manifest_metric: PydanticMetric) -> bool:
            """Check if the given metric and manifest_metric are equivalent based on selected fields."""
            fields_match = (
                search_metric.type == manifest_metric.type
                and search_metric.type_params.window == manifest_metric.type_params.window
                and search_metric.type_params.grain_to_date == manifest_metric.type_params.grain_to_date
                and search_metric.type_params.metric_aggregation_params
                == manifest_metric.type_params.metric_aggregation_params
                and search_metric.type_params.join_to_timespine == manifest_metric.type_params.join_to_timespine
                and search_metric.type_params.fill_nulls_with == manifest_metric.type_params.fill_nulls_with
                and search_metric.type_params.expr == manifest_metric.type_params.expr
                and search_metric.filter == manifest_metric.filter
                and search_metric.time_granularity == manifest_metric.time_granularity
            )
            if not fields_match:
                return False
            if (
                manifest_metric.type_params.measure is not None
                and search_metric.type_params.measure != manifest_metric.type_params.measure
            ):
                return False
            return True

        for existing_metric in manifest.metrics:
            if _metrics_equivalent(search_metric=metric, manifest_metric=existing_metric):
                return existing_metric
        return None

    @staticmethod
    def update_required_measure_features_in_simple_model(
        *,
        measure: PydanticMeasure,
        semantic_model_name: str,
        metric: PydanticMetric,
        # Measure input fields
        fill_nulls_with: Optional[int],
        join_to_timespine: Optional[bool],
        measure_input_filters: Optional[PydanticWhereFilterIntersection],
    ) -> None:
        """Set the measure features on the metric, as appropriate.

        Use this when merging an existing measure's
        arguments into a metrics.

        This will update the metric in place rather than returning a new one.
        """
        assert metric.type is MetricType.SIMPLE, f"Attempted to set measure features on a non-simple metric: {metric}"
        if metric.type_params.metric_aggregation_params is not None:
            # these values have already been set.
            return

        # We only set these if they are passed in explicitly so we can avoid overriding defaults.
        if fill_nulls_with is not None:
            metric.type_params.fill_nulls_with = fill_nulls_with
        if join_to_timespine:
            metric.type_params.join_to_timespine = join_to_timespine

        metric.type_params.metric_aggregation_params = PydanticMetric.build_metric_aggregation_params(
            measure=measure,
            semantic_model_name=semantic_model_name,
        )
        # Measures without an expr fall back to using the measure name as the column name,
        # so we need to enable mimicking that behavior here.
        if metric.type_params.expr is None:
            metric.type_params.expr = measure.expr or measure.name

        filters = measure_input_filters.where_filters if measure_input_filters else []
        if metric.filter is not None:
            filters.extend(metric.filter.where_filters)
        if len(filters) > 0:
            metric.filter = PydanticWhereFilterIntersection(where_filters=filters)

        # TODO SL-4257: this is supporting legacy cases in MF until work there is complete,
        # and should be removeable some time before the rest of the backward-compatibility work.
        artificial_measure_input = PydanticMetricInputMeasure(
            name=measure.name,
            filter=measure_input_filters,
            join_to_timespine=False,
            fill_nulls_with=None,
        )
        metric.type_params.measure = artificial_measure_input
        metric.type_params.input_measures = [artificial_measure_input]

    @staticmethod
    def build_metric_from_measure_configuration(
        measure: PydanticMeasure,
        semantic_model_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: Optional[bool],
        is_private: bool,
        measure_input_filters: Optional[PydanticWhereFilterIntersection],
    ) -> PydanticMetric:
        """Build a metric from the measure configuration.

        Name defaults to the measure name, which will require overriding in many cases
        (Name override is handled automatically if you are using
        get_or_create_metric_for_measure instead of this method).
        """
        type_params = PydanticMetricTypeParams(
            is_private=is_private,
        )

        new_metric = PydanticMetric(
            name=measure.name,
            type=MetricType.SIMPLE,
            type_params=type_params,
            description=measure.description,
            label=measure.label,
            config=measure.config,
            metadata=measure.metadata,
        )

        MeasureFeaturesToMetricNameMapper.update_required_measure_features_in_simple_model(
            measure=measure,
            semantic_model_name=semantic_model_name,
            metric=new_metric,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
            measure_input_filters=measure_input_filters,
        )

        return new_metric

    def _generate_new_metric_name(
        self,
        measure_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
        manifest: PydanticSemanticManifest,
        existing_metric_names: Set[str],
    ) -> str:
        """Generate a new metric name for the measure configuration."""
        name_parts = [measure_name]
        if fill_nulls_with is not None:
            fill_nulls_name_part = str(fill_nulls_with) if fill_nulls_with >= 0 else f"neg_{abs(fill_nulls_with)}"
            name_parts.append(f"fill_nulls_with_{fill_nulls_name_part}")
        if join_to_timespine:
            name_parts.append("join_to_timespine")

        base_name = "_".join(name_parts)
        new_name = base_name
        count = 1
        while new_name in existing_metric_names:
            # one hopes people are not naming their metrics like this, but we'll just assume
            # someone has and avoid collisions.
            new_name = f"{base_name}_{count}"
            count += 1

        return new_name

    def get_or_create_metric_for_measure(
        self,
        *,
        manifest: PydanticSemanticManifest,
        model_name: str,
        measure: PydanticMeasure,
        measure_input_filters: Optional[PydanticWhereFilterIntersection],
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
        existing_metric_names: Optional[Set[str]] = None,
    ) -> str:
        """Find the existing metric for a measure configuration, or create it if it doesn't exist.

        existing_metric_names should match the names of all metrics in the manifest;
            it's provided so that in cases where we're creating a lot of metrics in one go, we can
            avoid looping through the manifest's metrics extra times.  If provided, new metric
            names will be appended to this set as we go.

        returns the name of the metric
        """
        existing_metric_names = existing_metric_names or set([metric.name for metric in manifest.metrics])

        # Check: do we already have this in the dict?  Let's skip searching for it then!
        stored_metric_name = self._get_stored_metric_name(
            measure_name=measure.name,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
        )
        if stored_metric_name is not None:
            return stored_metric_name

        # if no, does a metric exist in the manifest that matches all required features?
        built_metric = self.build_metric_from_measure_configuration(
            measure=measure,
            semantic_model_name=model_name,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
            is_private=True,
            measure_input_filters=measure_input_filters,
        )
        metric = self._find_simple_metric_functional_clone_in_manifest(
            metric=built_metric,
            manifest=manifest,
        )

        if metric is None:
            # if we didn't find it, let's make a new name and add it to the manifest
            metric_name = self._generate_new_metric_name(
                measure_name=measure.name,
                fill_nulls_with=fill_nulls_with,
                join_to_timespine=join_to_timespine,
                manifest=manifest,
                existing_metric_names=existing_metric_names,
            )
            metric = built_metric
            metric.name = metric_name
            manifest.metrics.append(metric)
            existing_metric_names.add(metric_name)

        self._store_metric_name(
            measure_name=measure.name,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
            metric_name=metric.name,
        )
        return metric.name
