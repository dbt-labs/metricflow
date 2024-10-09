from __future__ import annotations

from enum import Enum
from typing import Any, FrozenSet


class LinkableElementProperty(Enum):
    """The properties associated with a valid linkable element.

    Local means an element that is defined within the same semantic model as the measure. This definition is used
    throughout the related classes.
    """

    # A local element as per above definition.
    LOCAL = "local"
    # A local dimension that is prefixed with a local primary entity.
    LOCAL_LINKED = "local_linked"
    # An element that was joined to the measure semantic model by an entity.
    JOINED = "joined"
    # An element that was joined to the measure semantic model by joining multiple semantic models.
    MULTI_HOP = "multi_hop"
    # A time dimension that is a version of a time dimension in a semantic model, but at a different granularity.
    DERIVED_TIME_GRANULARITY = "derived_time_granularity"
    # Refers to an entity, not a dimension.
    ENTITY = "entity"
    # See metric_time in DataSet
    METRIC_TIME = "metric_time"
    # Refers to a metric, not a dimension.
    METRIC = "metric"
    # A time dimension with a DatePart.
    DATE_PART = "date_part"
    # A linkable element that is itself part of an SCD model, or a linkable element that gets joined through another SCD model.
    SCD_HOP = "scd_hop"

    @staticmethod
    def all_properties() -> FrozenSet[LinkableElementProperty]:  # noqa: D102
        return frozenset({linkable_element_property for linkable_element_property in LinkableElementProperty})

    def __lt__(self, other: Any) -> bool:  # type: ignore[misc]
        """When ordering, order by the enum name."""
        if not isinstance(other, LinkableElementProperty):
            return NotImplemented

        return self.name < other.name
