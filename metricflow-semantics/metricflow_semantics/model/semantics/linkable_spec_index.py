from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Set

from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    SemanticModelReference,
)

from metricflow_semantics.model.semantics.linkable_element import (
    MetricSubqueryJoinPathElement,
)
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet

if TYPE_CHECKING:
    pass


from dataclasses import dataclass


@dataclass
class LinkableSpecIndex:
    """Contains items used by `ValidLinkableSpecResolver` during resolution.

    Potential use case for this index is that it can be stored to reduce initialization times.
    This was made as a mutable object to simplify migration, but this should be re-evalated.
    """

    metric_to_linkable_element_sets: Dict[str, List[LinkableElementSet]]
    no_metric_linkable_element_set: LinkableElementSet
    joinable_metrics_for_entities: Dict[EntityReference, Set[MetricSubqueryJoinPathElement]]
    semantic_model_reference_to_joined_elements: Dict[SemanticModelReference, LinkableElementSet]
    semantic_model_reference_to_local_elements: Dict[SemanticModelReference, LinkableElementSet]
    measure_to_metric_time_elements: Dict[Optional[MeasureReference], LinkableElementSet]
