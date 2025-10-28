from __future__ import annotations

import logging

from metricflow_semantics.errors.error_classes import MetricFlowInternalError

logger = logging.getLogger(__name__)


class SemanticGraphTraversalError(MetricFlowInternalError):
    """Raised when an unexpected condition is encountered during semantic-graph traversal."""

    pass
