from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.references import (
    MeasureReference,
    MetricReference,
)

from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet

logger = logging.getLogger(__name__)


class LinkableSpecResolver(ABC):
    """Resolves available group-by items for measures and metrics."""

    @abstractmethod
    def get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter,
    ) -> BaseGroupByItemSet:
        """Get the valid linkable elements for the given measure."""
        raise NotImplementedError

    @abstractmethod
    def get_linkable_elements_for_distinct_values_query(
        self,
        element_filter: LinkableElementFilter,
    ) -> BaseGroupByItemSet:
        """Returns queryable items for a distinct group-by-item values query.

        A distinct group-by-item values query does not include any metrics.
        """
        raise NotImplementedError

    @abstractmethod
    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> BaseGroupByItemSet:
        """Gets the valid linkable elements that are common to all requested metrics.

        The results of this method don't actually match what will be allowed for the metric because resolution goes
        through a separate and more comprehensive resolution process (`GroupByItemResolver`).
        # TODO: Consolidate resolution processes.
        """
        raise NotImplementedError
