from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Sequence, Tuple, TypeVar

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import EntityReference, LinkableElementReference

from metricflow_semantics.model.semantics.linkable_element import ElementPathKey
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName

if typing.TYPE_CHECKING:
    from metricflow_semantics.specs.dimension_spec import DimensionSpec
    from metricflow_semantics.specs.entity_spec import EntitySpec
    from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
    from metricflow_semantics.specs.measure_spec import MeasureSpec
    from metricflow_semantics.specs.metadata_spec import MetadataSpec
    from metricflow_semantics.specs.metric_spec import MetricSpec
    from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class InstanceSpec(SerializableDataclass):
    """A specification for an instance of a metric definition object.

    An instance is different from the definition object in that it correlates to columns in the data flow and can be in
    different states. e.g. a time dimension at a different time granularity.

    This can't be a Protocol as base classes of Protocols need to be Protocols.
    """

    """Name of the dimension or entity in the semantic model."""
    element_name: str

    @staticmethod
    def merge(*specs: Sequence[InstanceSpec]) -> List[InstanceSpec]:
        """Merge all specs into a single list."""
        result: List[InstanceSpec] = []
        for spec in specs:
            result.extend(spec)
        return result

    @property
    def qualified_name(self) -> str:
        """Return the qualified name of this spec. e.g. "user_id__country"."""
        raise NotImplementedError()

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:
        """See Visitable."""
        raise NotImplementedError()

    def without_filter_specs(self) -> InstanceSpec:
        """Return the instance spec without any filtering (for comparison purposes)."""
        return self


class InstanceSpecVisitor(Generic[VisitorOutputT], ABC):
    """Visitor for the InstanceSpec classes."""

    @abstractmethod
    def visit_measure_spec(self, measure_spec: MeasureSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_entity_spec(self, entity_spec: EntitySpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_group_by_metric_spec(self, group_by_metric_spec: GroupByMetricSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metric_spec(self, metric_spec: MetricSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


@dataclass(frozen=True)
class LinkableInstanceSpec(InstanceSpec, ABC):
    """Generally a dimension or entity that may be specified using entity links.

    For example, user_id__country -> LinkableElementSpec(element_name="country", entity_links=["user_id"]

    See InstanceSpec for the reason behind "type: ignore"
    """

    """A list representing the join path of entities to get to this element."""
    entity_links: Tuple[EntityReference, ...]

    @property
    @abstractmethod
    def without_first_entity_link(self: SelfTypeT) -> SelfTypeT:
        """e.g. user_id__device_id__platform -> device_id__platform."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def without_entity_links(self: SelfTypeT) -> SelfTypeT:
        """e.g. user_id__device_id__platform -> platform."""
        raise NotImplementedError()

    @staticmethod
    def merge_linkable_specs(*specs: Sequence[LinkableInstanceSpec]) -> List[LinkableInstanceSpec]:
        """Merge all specs into a single list."""
        result: List[LinkableInstanceSpec] = []
        for spec in specs:
            result.extend(spec)
        return result

    @property
    def qualified_name(self) -> str:
        """Return the qualified name of this spec. e.g. "user_id__country"."""
        return StructuredLinkableSpecName(
            entity_link_names=tuple(x.element_name for x in self.entity_links), element_name=self.element_name
        ).qualified_name

    @property
    @abstractmethod
    def reference(self) -> LinkableElementReference:
        """Return the LinkableElementReference associated with the spec instance."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def element_path_key(self) -> ElementPathKey:
        """Return the ElementPathKey representation of the LinkableInstanceSpec subtype."""
        raise NotImplementedError()

    @abstractmethod
    def with_entity_prefix(self, entity_prefix: EntityReference) -> LinkableInstanceSpec:
        """Add the selected entity prefix to the start of the entity links."""
        raise NotImplementedError()


SelfTypeT = TypeVar("SelfTypeT", bound="LinkableInstanceSpec")
