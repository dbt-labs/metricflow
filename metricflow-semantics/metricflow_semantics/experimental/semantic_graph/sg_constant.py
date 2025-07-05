from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ClusterName:
    TIME = "time"
    DSI_ENTITY = "dsi_entity"
    KEY = "key"
    GROUP_BY_METRIC = "group_by_metric"
    DIMENSION = "dimension"
    METRIC_ATTRIBUTE = "metric_attribute"
    TIME_DIMENSION = "time_dimension"
