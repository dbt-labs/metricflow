from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet

from metricflow.dataflow.builder.node_evaluator import JoinLinkableInstancesRecipe
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.nodes.join_to_base import JoinDescription


@dataclass(frozen=True)
class SourceNodeRecipe:
    """Get a recipe for how to build a dataflow plan node that outputs simple-metric inputs and linkable instances as needed."""

    source_node: DataflowPlanNode
    required_local_linkable_specs: LinkableSpecSet
    join_linkable_instances_recipes: Tuple[JoinLinkableInstancesRecipe, ...]
    all_linkable_specs_required_for_source_nodes: LinkableSpecSet

    @property
    def join_targets(self) -> List[JoinDescription]:
        """Joins to be made to source node."""
        return [join_recipe.join_description for join_recipe in self.join_linkable_instances_recipes]
