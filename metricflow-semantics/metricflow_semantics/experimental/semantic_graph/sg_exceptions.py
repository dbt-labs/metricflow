from __future__ import annotations

import logging

from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError

logger = logging.getLogger(__name__)


class SemanticGraphTraversalError(MetricflowInternalError):
    """Raised when an unexpected condition is encountered during semantic-graph traversal."""

    pass
