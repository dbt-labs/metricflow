from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import EntityReference, GroupByMetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.model.semantics.linkable_element import ElementPathKey, LinkableElementType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import InstanceSpecVisitor, LinkableInstanceSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class GroupByMetricSpec(LinkableInstanceSpec, SerializableDataclass):
    """Metric used in group by or where filter.

    Args:
        element_name: Name of the metric being joined.
        entity_links: Sequence of entities joined to join the metric subquery to the outer query. Last entity is the one
            joining the subquery to the outer query.
        metric_subquery_entity_links: Sequence of entities used in the metric subquery to join the metric to the entity.
    """

    metric_subquery_entity_links: Tuple[EntityReference, ...]

    def __post_init__(self) -> None:
        """The inner query and outer query entity paths must end with the same entity (that's what they join on).

        If no entity links, it's because we're already in the final joined node (no links left).
        """
        assert (
            len(self.metric_subquery_entity_links) > 0
        ), "GroupByMetricSpec must have at least one metric_subquery_entity_link."
        if self.entity_links:
            assert (
                self.metric_subquery_entity_links[-1] == self.entity_links[-1]
            ), "Inner and outer query must have the same last entity link in order to join on that link."

    @property
    def without_first_entity_link(self) -> GroupByMetricSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return GroupByMetricSpec(
            element_name=self.element_name,
            entity_links=self.entity_links[1:],
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )

    @property
    def without_entity_links(self) -> GroupByMetricSpec:  # noqa: D102
        return GroupByMetricSpec(
            element_name=self.element_name,
            entity_links=(),
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )

    @property
    def last_entity_link(self) -> EntityReference:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return self.entity_links[-1]

    @property
    def metric_subquery_entity_spec(self) -> EntitySpec:
        """Spec for the entity that the metric will be grouped by in the metric subquery."""
        assert (
            len(self.metric_subquery_entity_links) > 0
        ), "GroupByMetricSpec must have at least one metric_subquery_entity_link."
        return EntitySpec(
            element_name=self.metric_subquery_entity_links[-1].element_name,
            entity_links=self.metric_subquery_entity_links[:-1],
        )

    @property
    def qualified_name(self) -> str:
        """Element name prefixed with entity links.

        If same entity links are used in inner & outer query, use standard qualified name (country__bookings).
        Else, specify both sets of entity links (listing__country__user__country__bookings).
        """
        if self.entity_links == self.metric_subquery_entity_links:
            entity_links = self.entity_links
        else:
            entity_links = self.entity_links + self.metric_subquery_entity_links

        return StructuredLinkableSpecName(
            entity_link_names=tuple(entity_link.element_name for entity_link in entity_links),
            element_name=self.element_name,
        ).qualified_name

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D105
        if not isinstance(other, GroupByMetricSpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.element_name, self.entity_links, self.metric_subquery_entity_links))

    @property
    def reference(self) -> GroupByMetricReference:  # noqa: D102
        return GroupByMetricReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_group_by_metric_spec(self)

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.METRIC, entity_links=self.entity_links
        )

    def with_entity_prefix(self, entity_prefix: EntityReference) -> GroupByMetricSpec:  # noqa: D102
        return GroupByMetricSpec(
            element_name=self.element_name,
            entity_links=(entity_prefix,) + self.entity_links,
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )
        
    def with_time_granularity(self, time_granularity: Optional[TimeGranularity]) -> GroupByMetricSpec:
        """Create a new GroupByMetricSpec with the specified time granularity.
        
        This is used when a metric filter includes 'metric_time' in its group_by, to ensure
        that the filter is applied using the same time granularity as the parent query.
        
        Args:
            time_granularity: The time granularity to apply to the metric filter.
            
        Returns:
            A new GroupByMetricSpec with the time granularity applied.
        """
        if time_granularity is None:
            return self
            
        # Create a new element name that includes the time granularity
        # This will be used in SQL rendering to ensure the filter is applied at the correct granularity
        expanded_granularity = ExpandedTimeGranularity.from_time_granularity(time_granularity)
        new_element_name = f"{self.element_name}__{expanded_granularity.name}"
        
        return GroupByMetricSpec(
            element_name=new_element_name,
            entity_links=self.entity_links,
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )
