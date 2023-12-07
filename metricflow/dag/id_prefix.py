from __future__ import annotations

from enum import Enum


class IdPrefix(Enum):
    """Enumerates the prefixes used for generating IDs.

    TODO: Move all ID prefixes here.
    """

    # Group by item resolution
    GROUP_BY_ITEM_RESOLUTION_DAG = "gbir"
    QUERY_GROUP_BY_ITEM_RESOLUTION_NODE = "qr"
    METRIC_GROUP_BY_ITEM_RESOLUTION_NODE = "mtr"
    MEASURE_GROUP_BY_ITEM_RESOLUTION_NODE = "msr"
    VALUES_GROUP_BY_ITEM_RESOLUTION_NODE = "vr"
