from __future__ import annotations

import html
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, FrozenSet, Iterable, List, Sequence, Tuple, override

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import EntityType, TimeGranularity

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import DisplayableGraphElement
from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph_old.graph_path.path_property import SemanticModelJoinOperation
from metricflow_semantics.formatting.formatting_helpers import mf_dedent
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat


class Cardinality(Enum):
    ONE = "one"
    MANY = "many"

    @staticmethod
    def get_for_entity_type(entity_type: EntityType) -> Cardinality:
        if entity_type is EntityType.PRIMARY or entity_type is EntityType.NATURAL or entity_type is EntityType.UNIQUE:
            return Cardinality.ONE
        elif entity_type is EntityType.FOREIGN:
            return Cardinality.MANY


@dataclass(frozen=True)
class SemanticEdgeTypeProperties:
    displayed_properties: Tuple[DisplayedProperty, ...]


class SemanticGraphEdgeType(Enum):
    """Enumerates the possible entity relationships in the semantic graph."""

    METRIC_ATTRIBUTE = "metric_attribute"
    MEASURE_ATTRIBUTE = "measure_attribute"
    ATTRIBUTE = "attribute"
    ATTRIBUTE_SOURCE = "attribute_source"

    COMPOSITE = "composite"

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"

    def __lt__(self, other: Any) -> bool:  # type: ignore  # noqa: D105
        if not isinstance(other, SemanticGraphEdgeType):
            return NotImplemented
        return self.name < other.name

    @staticmethod
    def get_for_entity_types(tail_entity_type: EntityType, head_entity_type: EntityType) -> SemanticGraphEdgeType:
        tail_end_type = Cardinality.get_for_entity_type(tail_entity_type)
        head_end_type = Cardinality.get_for_entity_type(head_entity_type)

        if tail_end_type is Cardinality.ONE:
            if head_end_type is Cardinality.ONE:
                return SemanticGraphEdgeType.ONE_TO_ONE
            elif head_end_type is Cardinality.MANY:
                return SemanticGraphEdgeType.ONE_TO_MANY
            else:
                assert_values_exhausted(head_end_type)
        elif tail_end_type is Cardinality.MANY:
            if head_end_type is Cardinality.ONE:
                return SemanticGraphEdgeType.MANY_TO_ONE
            elif head_end_type is Cardinality.MANY:
                return SemanticGraphEdgeType.MANY_TO_MANY
            else:
                assert_values_exhausted(head_end_type)
        else:
            assert_values_exhausted(tail_end_type)

    # @property
    # def reverse_type(self) -> SemanticGraphEdgeType:
    #     if self is SemanticGraphEdgeType.ONE_TO_ONE:
    #         return SemanticGraphEdgeType.ONE_TO_ONE
    #     elif self is SemanticGraphEdgeType.ONE_TO_MANY:
    #         return SemanticGraphEdgeType.MANY_TO_ONE
    #     elif self is SemanticGraphEdgeType.MANY_TO_ONE:
    #         return SemanticGraphEdgeType.ONE_TO_MANY
    #     elif self is SemanticGraphEdgeType.MANY_TO_MANY:
    #         return SemanticGraphEdgeType.MANY_TO_MANY


class SemanticGraphEdgeTypeSet:
    ENTITY_RELATIONSHIP = frozenset(
        {
            SemanticGraphEdgeType.ONE_TO_ONE,
            SemanticGraphEdgeType.ONE_TO_MANY,
            SemanticGraphEdgeType.MANY_TO_ONE,
            SemanticGraphEdgeType.MANY_TO_MANY,
        }
    )


@dataclass(frozen=True)
class SemanticGraphPathStat:
    join_hop_count: int
    entity_link_count: int

    def with_incremented_join_hop_count(self) -> SemanticGraphPathStat:
        return SemanticGraphPathStat(
            join_hop_count=self.join_hop_count + 1,
            entity_link_count=self.entity_link_count,
        )

    def with_incremented_entity_link_count(self) -> SemanticGraphPathStat:
        return SemanticGraphPathStat(
            join_hop_count=self.join_hop_count,
            entity_link_count=self.entity_link_count,
        )


class SemanticGraphPathStatOperation(ABC):
    @abstractmethod
    def get_updated_stat(self, path_stat: SemanticGraphPathStat) -> SemanticGraphPathStat:
        raise NotImplementedError


class IncrementJoinHopCount(SemanticGraphPathStatOperation):
    @override
    def get_updated_stat(self, path_stat: SemanticGraphPathStat) -> SemanticGraphPathStat:
        return path_stat.with_incremented_join_hop_count()


class IncrementEntityLinkCount(SemanticGraphPathStatOperation):
    @override
    def get_updated_stat(self, path_stat: SemanticGraphPathStat) -> SemanticGraphPathStat:
        return path_stat.with_incremented_entity_link_count()


@dataclass(frozen=True)
class SemanticGraphEdge(DisplayableGraphElement, Comparable):
    """Describes a relationship between entities in the semantic graph"""

    tail_node: SemanticGraphNode
    head_node: SemanticGraphNode
    join_operations: Tuple[SemanticModelJoinOperation, ...]
    path_stat_operations: Tuple[SemanticGraphPathStatOperation, ...] = ()
    required_tags: RequiredTagSet = RequiredTagSet.empty_set()
    provided_tags: ProvidedEdgeTagSet = ProvidedEdgeTagSet.empty_set()

    def get_updated_path_stat(self, path_stat: SemanticGraphPathStat) -> SemanticGraphPathStat:
        for path_stat_operation in self.path_stat_operations:
            path_stat = path_stat_operation.get_updated_stat(path_stat)

        return path_stat

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return self.tail_node, self.head_node, self.join_operations

    @property
    @override
    def dot_label(self) -> str:
        return mf_pformat(
            {displayed_property.key: displayed_property.value for displayed_property in self.displayed_properties},
            max_line_length=60,
        )

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        # if self.edge_type is SemanticGraphEdgeType.ONE_TO_ONE:
        #     edge_type_name = "1->1"
        # elif self.edge_type is SemanticGraphEdgeType.ONE_TO_MANY:
        #     edge_type_name = "1->N"
        # elif self.edge_type is SemanticGraphEdgeType.MANY_TO_ONE:
        #     edge_type_name = "N->1"
        # elif self.edge_type is SemanticGraphEdgeType.MANY_TO_MANY:
        #     edge_type_name = "N->N"
        # else:
        #     assert_values_exhausted(self.edge_type)

        # displayed_properties = [DisplayedProperty("type", self.edge_type.name)]
        # displayed_properties.extend(self.join_operations.displayed_properties)
        displayed_properties: List[DisplayedProperty] = []
        for join_operation in self.join_operations:
            displayed_properties.extend(join_operation.displayed_properties)
        return displayed_properties

    @property
    @override
    def graphviz_label(self) -> str:
        table_lines = ['<FONT point-size="8">', '<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">']

        for displayed_property in self.displayed_properties:
            table_lines.append(
                indent(
                    f"<TR><TD ALIGN='LEFT'>{html.escape(displayed_property.key)}: {html.escape(displayed_property.value)}</TD></TR>",
                    indent_level=3,
                )
            )
        # table_lines.append(indent("</TR>", indent_level=1))
        table_lines.append("</TABLE>")
        table_lines.append("</FONT>")

        # Put the contents in an invisible HTML table to add some additional spacing between the label and edges.
        # There should be a way to do this via the API, but I was unable to find it.
        return "<" + self._invisible_table_html("\n".join(table_lines)) + ">"
        # return f"foo"

    def _invisible_table_html(self, contents: str) -> str:
        return mf_dedent(
            f"""
            <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
              <TR>
                <TD>{contents}</TD>
              </TR>
            </TABLE>
            """
        )


@dataclass(frozen=True)
class EdgeTimeGainAssociation:
    edge: SemanticGraphEdge
    time_grain: TimeGranularity


# def _displayed_property_value_for_grains(time_grains: Iterable[TimeGranularity]) -> str:
#     # TODO: Update this before PR.
#     # sorted_time_grains = sorted(time_grains, key=lambda time_grain: time_grain.to_int())
#     # if len(sorted_time_grains) == 0:
#     #     return "[]"
#     # return f">{min(sorted_time_grains).name}"
#     return str(list(time_grain.name for time_grain in time_grains))


def sorted_time_grain_names(time_grains: Iterable[TimeGranularity]) -> Sequence[str]:
    sorted_time_grains = sorted(time_grains, key=lambda time_grain: time_grain.to_int())
    return [time_grain.name for time_grain in sorted_time_grains]


@dataclass(frozen=True)
class ProvidedEdgeTagSet:
    attribute_time_grains: FrozenSet[TimeGranularity]
    metric_time_grains: FrozenSet[TimeGranularity]

    def intersection(self, other: ProvidedEdgeTagSet) -> ProvidedEdgeTagSet:
        return ProvidedEdgeTagSet(
            attribute_time_grains=self.attribute_time_grains.intersection(other.attribute_time_grains),
            metric_time_grains=self.metric_time_grains.intersection(other.metric_time_grains),
        )

    @staticmethod
    def create(
        attribute_time_grains: Iterable[TimeGranularity] = (),
        metric_time_grains: Iterable[TimeGranularity] = (),
    ) -> ProvidedEdgeTagSet:
        return ProvidedEdgeTagSet(
            attribute_time_grains=frozenset(attribute_time_grains),
            metric_time_grains=frozenset(metric_time_grains),
        )

    @staticmethod
    def empty_set() -> ProvidedEdgeTagSet:
        return ProvidedEdgeTagSet(attribute_time_grains=frozenset(), metric_time_grains=frozenset())

    @property
    def empty(self) -> bool:
        return len(self.metric_time_grains) + len(self.attribute_time_grains) == 0

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        displayed_properties = []
        if len(self.attribute_time_grains) > 0:
            displayed_properties.append(
                DisplayedProperty(
                    "attribute_time_grains", ">=" + sorted_time_grain_names(self.attribute_time_grains)[0]
                )
            )
        if len(self.metric_time_grains) > 0:
            displayed_properties.append(
                DisplayedProperty("metric_time_grains", ">=" + sorted_time_grain_names(self.metric_time_grains)[0])
            )
        return displayed_properties


@dataclass(frozen=True)
class RequiredTagSet:
    allowed_attribute_time_grains: FrozenSet[TimeGranularity]

    @staticmethod
    def create(
        allowed_attribute_time_grains: Iterable[TimeGranularity],
    ) -> RequiredTagSet:
        return RequiredTagSet(
            allowed_attribute_time_grains=frozenset(allowed_attribute_time_grains),
        )

    @staticmethod
    def empty_set() -> RequiredTagSet:
        return RequiredTagSet(allowed_attribute_time_grains=frozenset())

    @property
    def empty(self) -> bool:
        return len(self.allowed_attribute_time_grains) == 0

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        displayed_properties = []
        if len(self.allowed_attribute_time_grains) > 0:
            displayed_properties.append(
                DisplayedProperty(
                    "allowed_attribute_time_grains",
                    "<=" + sorted_time_grain_names(self.allowed_attribute_time_grains)[-1],
                )
            )

        return displayed_properties
