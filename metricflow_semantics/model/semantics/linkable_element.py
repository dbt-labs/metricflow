from __future__ import annotations

import logging

from metricflow_semantics.toolkit.orderd_enum import OrderedEnum

logger = logging.getLogger(__name__)


class LinkableElementType(OrderedEnum):
    """Enumeration of the possible types of linkable element we are encountering or expecting.

    Group-by items effectively map on to LinkableSpecs and queryable semantic manifest elements such
    as Metrics, Dimensions, and Entities. This provides the full set of types we might encounter, and is
    useful for ensuring that we are always getting the correct LinkableElement from a given part of the
    codebase - e.g., to ensure we are not accidentally getting an Entity when we expect a Dimension.
    """

    DIMENSION = "dimension"
    ENTITY = "entity"
    METRIC = "metric"
    TIME_DIMENSION = "time_dimension"
