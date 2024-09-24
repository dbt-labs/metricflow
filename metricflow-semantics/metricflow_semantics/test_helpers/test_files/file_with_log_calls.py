from __future__ import annotations

import logging

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)

logger.info(LazyFormat("foo"))
