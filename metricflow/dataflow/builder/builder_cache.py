from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.where_filter.where_filter_transform import WhereSpecFactory
from metricflow_semantics.toolkit.cache.lru_cache import LruCache

from metricflow.dataflow.builder.simple_metric_input_spec_properties import SimpleMetricInputSpecProperties
from metricflow.dataflow.builder.source_node_recipe import SourceNodeRecipe
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.plan_conversion.node_processor import PredicatePushdownState


@dataclass(frozen=True)
class FindSourceNodeRecipeParameterSet:
    """Parameters for `DataflowPlanBuilder._find_source_node_recipe()`."""

    linkable_spec_set: LinkableSpecSet
    predicate_pushdown_state: PredicatePushdownState
    spec_properties: Optional[SimpleMetricInputSpecProperties]


@dataclass(frozen=True)
class FindSourceNodeRecipeResult:
    """Result for `DataflowPlanBuilder._find_source_node_recipe()`."""

    source_node_recipe: Optional[SourceNodeRecipe]


@dataclass(frozen=True)
class BuildAnyMetricOutputNodeParameterSet:
    """Parameters for `DataflowPlanBuilder._build_any_metric_output_node()`."""

    metric_spec: MetricSpec
    queried_linkable_specs: LinkableSpecSet
    filter_spec_factory: WhereSpecFactory
    predicate_pushdown_state: PredicatePushdownState
    for_group_by_source_node: bool


class DataflowPlanBuilderCache:
    """Cache for internal methods in `DataflowPlanBuilder`."""

    def __init__(  # noqa: D107
        self, find_source_node_recipe_cache_size: int = 1000, build_any_metric_output_node_cache_size: int = 1000
    ) -> None:
        self._find_source_node_recipe_cache = LruCache[FindSourceNodeRecipeParameterSet, FindSourceNodeRecipeResult](
            find_source_node_recipe_cache_size
        )
        self._build_any_metric_output_node_cache = LruCache[BuildAnyMetricOutputNodeParameterSet, DataflowPlanNode](
            build_any_metric_output_node_cache_size
        )

        assert find_source_node_recipe_cache_size > 0
        assert build_any_metric_output_node_cache_size > 0

    def get_find_source_node_recipe_result(  # noqa: D102
        self, parameter_set: FindSourceNodeRecipeParameterSet
    ) -> Optional[FindSourceNodeRecipeResult]:
        return self._find_source_node_recipe_cache.get(parameter_set)

    def set_find_source_node_recipe_result(  # noqa: D102
        self, parameter_set: FindSourceNodeRecipeParameterSet, source_node_recipe: FindSourceNodeRecipeResult
    ) -> None:
        self._find_source_node_recipe_cache.set(parameter_set, source_node_recipe)

    def get_build_any_metric_output_node_result(  # noqa: D102
        self, parameter_set: BuildAnyMetricOutputNodeParameterSet
    ) -> Optional[DataflowPlanNode]:
        return self._build_any_metric_output_node_cache.get(parameter_set)

    def set_build_any_metric_output_node_result(  # noqa: D102
        self, parameter_set: BuildAnyMetricOutputNodeParameterSet, dataflow_plan_node: DataflowPlanNode
    ) -> None:
        self._build_any_metric_output_node_cache.set(parameter_set, dataflow_plan_node)
