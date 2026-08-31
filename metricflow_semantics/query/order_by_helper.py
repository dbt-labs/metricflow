from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from functools import cached_property

from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)


class OrderByHelper:
    """Helper to resolve specs for matching order-by items."""

    def __init__(
        self,
        metric_specs: Iterable[MetricSpec],
        group_by_item_specs: Iterable[LinkableInstanceSpec],
    ) -> None:
        """Initializer.

        Args:
            metric_specs: The metrics in the query with the alias set based on query input.
            group_by_item_specs: The group-by items in the query with the alias set based on query input.
        """
        specs_with_alias_context: list[MetricSpec | LinkableInstanceSpec] = list(metric_specs)
        specs_with_alias_context.extend(group_by_item_specs)
        self._alias_to_specs: defaultdict[
            str | None, MutableOrderedSet[MetricSpec | LinkableInstanceSpec]
        ] = defaultdict(MutableOrderedSet)

        for spec in specs_with_alias_context:
            self._alias_to_specs[spec.alias].add(spec.with_alias(None))

    def specs_with_alias(self, alias: str | None) -> OrderedSet[MetricSpec | LinkableInstanceSpec]:
        """Returns the specs in the query that were specified with the given alias.

        Specs are returned with the alias stripped for matching using spec patterns.
        """
        return self._alias_to_specs[alias]

    @cached_property
    def all_specs(self) -> OrderedSet[MetricSpec | LinkableInstanceSpec]:
        """Returns all specs in the query.

        Specs are returned with the alias stripped for matching using spec patterns.
        """
        return FrozenOrderedSet(spec for specs in self._alias_to_specs.values() for spec in specs)
