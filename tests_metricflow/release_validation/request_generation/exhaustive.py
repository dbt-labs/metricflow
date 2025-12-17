from __future__ import annotations

import itertools
from collections import defaultdict
from typing import Mapping, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.protocols.query_parameter import GroupByQueryParameter
from metricflow_semantics.specs.query_param_implementations import (
    DimensionOrEntityParameter,
    MetricParameter,
    TimeDimensionParameter,
)
from metricflow_semantics.toolkit.collections.sequence_helpers import mf_chunk
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from tests_metricflow.release_validation.request_generation.request_generator import MetricFlowRequestGenerator


class ExhaustiveQueryGenerator(MetricFlowRequestGenerator):
    """Exhaustively generate requests for combinations of metrics and group-by items."""

    def __init__(self, metric_chunk_size: int, group_by_item_chunk_size: int) -> None:  # noqa: D107
        # The chunk size controls how many metrics and group by items are put into a single request.
        # Additional work is needed to generate all possible combinations.
        self._metric_chunk_size = metric_chunk_size
        self._group_by_item_chunk_size = group_by_item_chunk_size

    @override
    def generate_requests(self, semantic_manifest: SemanticManifest) -> Sequence[MetricFlowQueryRequest]:
        manifest_lookup = SemanticManifestLookup(semantic_manifest)
        available_metric_references = tuple(MetricReference(metric.name) for metric in semantic_manifest.metrics)
        mf_requests = []
        for metric_references in mf_chunk(available_metric_references, self._metric_chunk_size):
            element_type_to_parameters = self._resolve_possible_group_by_items(
                manifest_lookup.metric_lookup, metric_references
            )
            all_parameters = tuple(itertools.chain(*(parameters for parameters in element_type_to_parameters.values())))

            requires_metric_time = any(
                self._requires_metric_time(manifest_lookup.metric_lookup, metric_reference)
                for metric_reference in metric_references
            )
            for group_by_parameters in mf_chunk(all_parameters, self._group_by_item_chunk_size):
                metrics = tuple(
                    MetricParameter(metric_reference.element_name) for metric_reference in metric_references
                )
                group_by = list(group_by_parameters)
                if requires_metric_time and not any(
                    group_by_parameter.name == METRIC_TIME_ELEMENT_NAME for group_by_parameter in group_by_parameters
                ):
                    group_by.append(TimeDimensionParameter(name=METRIC_TIME_ELEMENT_NAME))

                mf_requests.append(
                    MetricFlowQueryRequest.create_with_random_request_id(metrics=metrics, group_by=group_by)
                )
        return mf_requests

    def _resolve_possible_group_by_items(
        self, metric_lookup: MetricLookup, metric_references: Sequence[MetricReference]
    ) -> Mapping[LinkableElementType, Sequence[GroupByQueryParameter]]:
        available_group_by_set = metric_lookup.get_common_group_by_items(
            metric_references,
            GroupByItemSetFilter.create(
                any_properties_denylist=(GroupByItemProperty.DATE_PART, GroupByItemProperty.METRIC)
            ),
        )

        element_type_to_parameters: defaultdict[LinkableElementType, list[GroupByQueryParameter]] = defaultdict(list)
        for annotated_spec in available_group_by_set.annotated_specs:
            element_type = annotated_spec.element_type
            dunder_name = StructuredLinkableSpecName(
                entity_link_names=annotated_spec.entity_link_names,
                element_name=annotated_spec.element_name,
            ).dunder_name
            if element_type is LinkableElementType.DIMENSION or element_type is LinkableElementType.ENTITY:
                element_type_to_parameters[element_type].append(DimensionOrEntityParameter(dunder_name))
            elif element_type is LinkableElementType.TIME_DIMENSION:
                time_grain = annotated_spec.time_grain
                if time_grain is None:
                    raise RuntimeError(LazyFormat("time_grain is None", annotated_spec=annotated_spec))
                element_type_to_parameters[element_type].append(
                    TimeDimensionParameter(
                        dunder_name,
                        grain=time_grain.name,
                    )
                )
            elif element_type is LinkableElementType.METRIC:
                # Group-by-metrics are only used in filters.
                pass
            else:
                assert_values_exhausted(element_type)
        return element_type_to_parameters

    def _requires_metric_time(self, metric_lookup: MetricLookup, metric_reference: MetricReference) -> bool:
        """Some types of metrics require `metric_time` to be present in the query.

        TODO: Cache results.
        """
        metric = metric_lookup.get_metric(metric_reference)
        metric_type = metric.type

        if metric_type is MetricType.SIMPLE or metric_type is MetricType.RATIO or metric_type is MetricType.CONVERSION:
            return False
        elif metric_type is MetricType.CUMULATIVE:
            return True
        elif metric_type is MetricType.DERIVED:
            input_metrics = metric.type_params.metrics
            assert input_metrics is not None
            for input_metric in input_metrics:
                if input_metric.offset_to_grain or input_metric.offset_window:
                    return True
            for input_metric in input_metrics:
                if self._requires_metric_time(metric_lookup, MetricReference(input_metric.name)):
                    return True
            return False

        assert_values_exhausted(metric_type)
