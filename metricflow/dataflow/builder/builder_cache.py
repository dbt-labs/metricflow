from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.toolkit.cache.lru_cache import LruCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.dataflow.builder.source_node_recipe import SourceNodeRecipe
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.plan_conversion.node_processor import PredicatePushdownState

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FindSourceNodeRecipeInput:
    """Parameters for `DataflowPlanBuilder._find_source_node_recipe()`."""

    simple_metric_input_specs: Optional[AnyLengthTuple[SimpleMetricInputSpec]]
    linkable_spec_set: LinkableSpecSet
    predicate_pushdown_state: PredicatePushdownState


@dataclass(frozen=True)
class FindSourceNodeRecipeResult:
    """Result for `DataflowPlanBuilder._find_source_node_recipe()`."""

    source_node_recipe: Optional[SourceNodeRecipe]


@dataclass(frozen=True)
class BuildAnyMetricOutputNodeInput:
    """Parameters for `DataflowPlanBuilder._build_any_metric_output_node()`."""

    metric_query_descriptor: MetricQueryDescriptor
    output_group_by_metric_instances: bool


class DataflowPlanBuilderCache:
    """Cache for internal methods in `DataflowPlanBuilder`."""

    def __init__(  # noqa: D107
        self, find_source_node_recipe_cache_size: int = 1000, build_any_metric_output_node_cache_size: int = 1000
    ) -> None:
        self._find_source_node_recipe_cache = LruCache[FindSourceNodeRecipeInput, FindSourceNodeRecipeResult](
            find_source_node_recipe_cache_size
        )
        self._build_any_metric_output_node_cache = LruCache[BuildAnyMetricOutputNodeInput, DataflowPlanNode](
            build_any_metric_output_node_cache_size
        )

        assert find_source_node_recipe_cache_size > 0
        assert build_any_metric_output_node_cache_size > 0

    def get_find_source_node_recipe_result(  # noqa: D102
        self, cache_key: FindSourceNodeRecipeInput
    ) -> Optional[FindSourceNodeRecipeResult]:
        return self._find_source_node_recipe_cache.get(cache_key)

    def set_find_source_node_recipe_result(  # noqa: D102
        self, cache_key: FindSourceNodeRecipeInput, source_node_recipe: FindSourceNodeRecipeResult
    ) -> None:
        self._find_source_node_recipe_cache.set(cache_key, source_node_recipe)

    def get_build_any_metric_output_node_result(  # noqa: D102
        self, cache_key: BuildAnyMetricOutputNodeInput
    ) -> Optional[DataflowPlanNode]:
        return self._build_any_metric_output_node_cache.get(cache_key)

    def set_build_any_metric_output_node_result(  # noqa: D102
        self, cache_key: BuildAnyMetricOutputNodeInput, dataflow_plan_node: DataflowPlanNode
    ) -> None:
        self._build_any_metric_output_node_cache.set(cache_key, dataflow_plan_node)


@fast_frozen_dataclass()
class MetricQueryDescriptor:
    """Describes a metric query to use as a cache key."""

    computed_metric_specs: FrozenOrderedSet[MetricSpec]
    passthrough_metric_specs: FrozenOrderedSet[MetricSpec]
    group_by_item_specs: FrozenOrderedSet[LinkableInstanceSpec]
    predicate_pushdown_state: PredicatePushdownState

    @staticmethod
    def create(  # noqa: D102
        computed_metric_specs: Iterable[MetricSpec],
        passthrough_metric_specs: Iterable[MetricSpec],
        group_by_item_specs: Iterable[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
    ) -> MetricQueryDescriptor:
        return MetricQueryDescriptor(
            computed_metric_specs=FrozenOrderedSet.from_iterable(computed_metric_specs),
            passthrough_metric_specs=FrozenOrderedSet.from_iterable(passthrough_metric_specs),
            group_by_item_specs=FrozenOrderedSet.from_iterable(group_by_item_specs),
            predicate_pushdown_state=predicate_pushdown_state,
        )
