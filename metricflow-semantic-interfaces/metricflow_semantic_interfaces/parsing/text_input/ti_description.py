from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Tuple

from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.errors import InvalidQuerySyntax
from metricflow_semantic_interfaces.naming.dundered import StructuredDunderedName
from metricflow_semantic_interfaces.type_enums.date_part import DatePart


@dataclass(frozen=True)
class ObjectBuilderItemDescription:
    """Describes a query item specified by the user.

    For example, the following specified in an order-by of a saved query:

        Dimension("user__created_at", entity_path=['listing']).grain('day').date_part('month').descending(True)

        ->

        ObjectBuilderItemDescription(
            item_type=GroupByItemType.DIMENSION,
            item_name="user__created_at",
            entity_path=['listing'],
            time_granularity_name='day'
            date_part_name='month'
            descending=True
        )

    * This is named "...Description" to keep it general as the way users specify query items will change significantly
      in the not-so-distant future.
    * This can be later expanded to a set of classes for better typing.
    """

    item_type: QueryItemType
    item_name: str
    entity_path: Tuple[str, ...]
    group_by_for_metric_item: Tuple[str, ...]
    time_granularity_name: Optional[str]
    date_part_name: Optional[str]
    descending: Optional[bool]

    def __post_init__(self) -> None:  # noqa: D105
        item_type = self.item_type

        # Check that time granularity and date part are only specified for dimensions and time dimensions.
        if item_type is QueryItemType.ENTITY or item_type is QueryItemType.METRIC:
            if self.time_granularity_name is not None:
                raise InvalidQuerySyntax(f"{self.time_granularity_name=} is not supported for {item_type=}")
            if self.date_part_name is not None:
                raise InvalidQuerySyntax(f"{self.date_part_name=} is not supported for {item_type=}")
        elif item_type is QueryItemType.TIME_DIMENSION or item_type is QueryItemType.DIMENSION:
            pass
        else:
            assert_values_exhausted(item_type)

        # Check that metrics do not have an entity prefix or entity path.
        if item_type is QueryItemType.METRIC:
            if len(self.entity_path) > 0:
                raise InvalidQuerySyntax("The entity path should not be specified for a metric.")
            if (
                len(StructuredDunderedName.parse_name(name=self.item_name, custom_granularity_names=()).entity_links)
                > 0
            ):
                raise InvalidQuerySyntax("The name of the metric should not have entity links.")
        # Check that dimensions / time dimensions have a valid date part.
        elif item_type is QueryItemType.DIMENSION or item_type is QueryItemType.TIME_DIMENSION:
            if self.date_part_name is not None:
                valid_date_part_names = set(date_part.value for date_part in DatePart)
                if self.date_part_name.lower() not in set(date_part.value for date_part in DatePart):
                    raise InvalidQuerySyntax(
                        f"{self.date_part_name!r} is not a valid date part. Valid values are"
                        f" {valid_date_part_names}"
                    )

            # Check that non-metric items don't specify group_by_for_metric_item.
            if item_type is QueryItemType.METRIC:
                pass
            elif (
                item_type is QueryItemType.DIMENSION
                or item_type is QueryItemType.ENTITY
                or item_type is QueryItemType.TIME_DIMENSION
            ):
                if len(self.group_by_for_metric_item) > 0:
                    raise InvalidQuerySyntax("A group-by should only be specified for metrics.")
            else:
                assert_values_exhausted(item_type)

    def create_modified(
        self,
        time_granularity_name: Optional[str] = None,
        date_part_name: Optional[str] = None,
        descending: Optional[bool] = None,
    ) -> ObjectBuilderItemDescription:
        """Create one with the same fields as self except the ones provided."""
        return ObjectBuilderItemDescription(
            item_type=self.item_type,
            item_name=self.item_name,
            entity_path=self.entity_path,
            time_granularity_name=time_granularity_name or self.time_granularity_name,
            date_part_name=date_part_name or self.date_part_name,
            group_by_for_metric_item=self.group_by_for_metric_item,
            descending=descending or self.descending,
        )

    def with_descending_unset(self) -> ObjectBuilderItemDescription:
        """Return this with the `descending` field set to None."""
        return ObjectBuilderItemDescription(
            item_type=self.item_type,
            item_name=self.item_name,
            entity_path=self.entity_path,
            time_granularity_name=self.time_granularity_name,
            date_part_name=self.date_part_name,
            group_by_for_metric_item=self.group_by_for_metric_item,
            descending=None,
        )


class QueryItemType(Enum):
    """Enumerates the types of items that a used to group items in a filter or a query.

    e.g. in the object-builder syntax: QueryItemType.DIMENSION refers to `Dimension(...)`.

    The value of the enum is the name of the builder "object".
    """

    DIMENSION = "Dimension"
    TIME_DIMENSION = "TimeDimension"
    ENTITY = "Entity"
    METRIC = "Metric"

    def __lt__(self, other: Any) -> bool:  # type: ignore[misc]
        """Allow for ordering so that a sequence of these can be consistently represented for test snapshots."""
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class ObjectBuilderMethod(Enum):
    """In the object builder notation, the possible methods that can be called on the builder object.

    e.g. ObjectBuilderMethod.GRAIN refers to `.grain` in `Dimension(...).grain('month')`

    The value of the enum is the name of the method.
    """

    GRAIN = "grain"
    DATE_PART = "date_part"
    DESCENDING = "descending"
