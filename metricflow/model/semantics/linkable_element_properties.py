from __future__ import annotations

from enum import Enum
from typing import FrozenSet


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

    @staticmethod
    def all_properties() -> FrozenSet[LinkableElementProperty]:  # noqa: D102
        return frozenset(
            {
                LinkableElementProperty.LOCAL,
                LinkableElementProperty.LOCAL_LINKED,
                LinkableElementProperty.JOINED,
                LinkableElementProperty.MULTI_HOP,
                LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                LinkableElementProperty.METRIC_TIME,
                LinkableElementProperty.METRIC,
            }
        )
