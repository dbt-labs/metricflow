from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from typing import Mapping, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.protocols.query_parameter import GroupByQueryParameter
from metricflow_semantics.specs.query_param_implementations import (
    DimensionOrEntityParameter,
    MetricParameter,
    TimeDimensionParameter,
)
from metricflow_semantics.toolkit.collections.sequence_helpers import mf_chunk
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class RandomQueryDescriptor:
    max_metric_count: int
    max_group_by_item_count: int

    group_by_dimension_weight: int
    group_by_entity_weight: int

    random_seed: int


class ExhaustiveQueryGenerator:
    def __init__(self, semantic_manifest: SemanticManifest) -> None:
        self._manifest = semantic_manifest
        self._manifest_lookup = SemanticManifestLookup(semantic_manifest)
        self._available_metric_references = tuple(MetricReference(metric.name) for metric in semantic_manifest.metrics)

    def generate_queries(self) -> Sequence[MetricFlowQueryRequest]:
        mf_requests = []
        for metric_reference in self._available_metric_references:
            element_type_to_parameters = self._resolve_possible_group_by_items(metric_reference)
            all_parameters = tuple(itertools.chain(*(parameters for parameters in element_type_to_parameters.values())))
            for group_by_parameters in mf_chunk(all_parameters, 20):
                mf_requests.append(
                    MetricFlowQueryRequest.create_with_random_request_id(
                        metrics=[MetricParameter(metric_reference.element_name)],
                        group_by=group_by_parameters,
                    )
                )
        return mf_requests

    # def _generate_for_one_metric(self, metric_reference: MetricReference) -> Iterable[MetricFlowQueryRequest]:
    def _resolve_possible_group_by_items(
        self, metric_reference: MetricReference
    ) -> Mapping[LinkableElementType, Sequence[GroupByQueryParameter]]:
        available_group_by_set = self._manifest_lookup.metric_lookup.get_common_group_by_items(
            [metric_reference],
            GroupByItemSetFilter.create(
                any_properties_denylist=(GroupByItemProperty.DATE_PART, GroupByItemProperty.METRIC)
            ),
        )

        accounted_group_by_item_keys: set[AnyLengthTuple] = set()

        element_type_to_parameters: defaultdict[LinkableElementType, list[GroupByQueryParameter]] = defaultdict(list)
        for annotated_spec in available_group_by_set.annotated_specs:
            # Dedupe to avoid querying time dimensions at different grains.
            group_by_item_key = annotated_spec.entity_link_names + (annotated_spec.element_name,)
            if group_by_item_key in accounted_group_by_item_keys:
                continue
            accounted_group_by_item_keys.add(group_by_item_key)

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


class SavedQueryGenerator:
    def __init__(self, semantic_manifest: SemanticManifest) -> None:
        self._manifest = semantic_manifest

    def generate_queries(self) -> Sequence[MetricFlowQueryRequest]:
        queries: list[MetricFlowQueryRequest] = []
        for saved_query in self._manifest.saved_queries:
            queries.append(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name))
        return queries
