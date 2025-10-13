from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Iterable, Optional

from dbt_semantic_interfaces.references import (
    MetricReference,
)

from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet

logger = logging.getLogger(__name__)


class GroupByItemSetResolver(ABC):
    """Resolves available group-by items for simple-metric inputs and metrics."""

    @abstractmethod
    def get_set_for_distinct_values_query(
        self,
        set_filter: Optional[GroupByItemSetFilter] = None,
    ) -> BaseGroupByItemSet:
        """Returns queryable items for a distinct group-by-item values query.

        A distinct group-by-item values query does not include any metrics.
        """
        raise NotImplementedError

    @abstractmethod
    def get_common_set(
        self,
        metric_references: Iterable[MetricReference] = (),
        set_filter: Optional[GroupByItemSetFilter] = None,
        joins_disallowed: bool = False,
    ) -> BaseGroupByItemSet:
        """Gets the set of the valid group-by items common to all inputs."""
        raise NotImplementedError
