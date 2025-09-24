from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Sequence

from dbt_semantic_interfaces.references import (
    MeasureReference,
    MetricReference,
)

from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet

logger = logging.getLogger(__name__)


class GroupByItemSetResolver(ABC):
    """Resolves available group-by items for measures and metrics."""

    @abstractmethod
    def get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: GroupByItemSetFilter,
    ) -> BaseGroupByItemSet:
        """Get the valid linkable elements for the given measure."""
        raise NotImplementedError

    @abstractmethod
    def get_linkable_elements_for_distinct_values_query(
        self,
        element_filter: GroupByItemSetFilter,
    ) -> BaseGroupByItemSet:
        """Returns queryable items for a distinct group-by-item values query.

        A distinct group-by-item values query does not include any metrics.
        """
        raise NotImplementedError

    @abstractmethod
    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: GroupByItemSetFilter = GroupByItemSetFilter(),
    ) -> BaseGroupByItemSet:
        """Gets the valid linkable elements that are common to all requested metrics.

        The results of this method don't actually match what will be allowed for the metric because resolution goes
        through a separate and more comprehensive resolution process (`GroupByItemResolver`).
        # TODO: Consolidate resolution processes.
        """
        raise NotImplementedError

    @abstractmethod
    def get_common_set(
        self,
        measure_references: Iterable[MeasureReference] = (),
        metric_references: Iterable[MetricReference] = (),
        set_filter: Optional[GroupByItemSetFilter] = None,
    ) -> BaseGroupByItemSet:
        """Gets the set of the valid group-by items common to all inputs."""
        raise NotImplementedError
