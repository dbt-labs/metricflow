from __future__ import annotations

import logging
from enum import Enum
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.naming.naming_scheme import QueryItemNamingScheme
from metricflow_semantics.query.similarity import top_fuzzy_matches
from metricflow_semantics.specs.patterns.default_time_granularity import DefaultTimeGranularityPattern
from metricflow_semantics.specs.patterns.match_list_pattern import MatchListSpecPattern
from metricflow_semantics.specs.patterns.no_group_by_metric import NoGroupByMetricPattern
from metricflow_semantics.specs.patterns.none_date_part import NoneDatePartPattern
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_classes import InstanceSpec, LinkableInstanceSpec

logger = logging.getLogger(__name__)


class QueryPartForSuggestions(Enum):
    """Indicates which type of query parameter is being suggested."""

    WHERE_FILTER = "where_filter"
    GROUP_BY = "group_by"
    METRIC = "metric"


class QueryItemSuggestionGenerator:
    """Returns specs that partially match a spec pattern created from user input. Used for suggestions in errors.

    Since suggestions are needed for group-by-items specified in the query and in where filters, an optional candidate
    filter can be specified to limit suggestions to the ones valid for the entire query. For use with where filters,
    a candidate filter is not needed as any available spec at a resolution node can be used.
    """

    def __init__(  # noqa: D107
        self,
        input_naming_scheme: QueryItemNamingScheme,
        input_str: str,
        query_part: QueryPartForSuggestions,
        metric_lookup: MetricLookup,
        queried_metrics: Sequence[MetricReference],
        valid_group_by_item_specs_for_querying: Optional[Sequence[LinkableInstanceSpec]] = None,
    ) -> None:
        self._input_naming_scheme = input_naming_scheme
        self._input_str = input_str
        self._query_part = query_part
        self._metric_lookup = metric_lookup
        self._queried_metrics = queried_metrics
        self._valid_group_by_item_specs_for_querying = valid_group_by_item_specs_for_querying

        if self._query_part is QueryPartForSuggestions.GROUP_BY and valid_group_by_item_specs_for_querying is None:
            raise ValueError(
                "QueryItemSuggestionGenerator requires valid_group_by_item_specs_for_querying param when used on group by items."
            )

    @property
    def candidate_filters(self) -> Tuple[SpecPattern, ...]:
        """Filters to apply before determining suggestions.

        These eensure we don't get multiple suggestions that are similar, but with different grains or date_parts.
        """
        default_filters = (
            DefaultTimeGranularityPattern(metric_lookup=self._metric_lookup, queried_metrics=self._queried_metrics),
            NoneDatePartPattern(),
        )
        if self._query_part is QueryPartForSuggestions.WHERE_FILTER:
            return default_filters
        elif self._query_part is QueryPartForSuggestions.GROUP_BY:
            assert self._valid_group_by_item_specs_for_querying, (
                "Group by suggestions require valid_group_by_item_specs_for_querying param."
                "This should have been validated on init."
            )
            return default_filters + (
                NoGroupByMetricPattern(),
                MatchListSpecPattern(
                    listed_specs=self._valid_group_by_item_specs_for_querying,
                ),
            )
        elif self._query_part is QueryPartForSuggestions.METRIC:
            return ()
        else:
            assert_values_exhausted(self._query_part)

    def input_suggestions(
        self,
        candidate_specs: Sequence[InstanceSpec],
        max_suggestions: int = 6,
    ) -> Sequence[str]:
        """Return the best specs that match the given pattern from candidate_specs and match the candidate_filter."""
        for candidate_filter in self.candidate_filters:
            candidate_specs = candidate_filter.match(candidate_specs)

        # Use edit distance to figure out the closest matches, so convert the specs to strings.
        candidate_strs = set()
        for candidate_spec in candidate_specs:
            candidate_str = self._input_naming_scheme.input_str(candidate_spec)

            if candidate_str is not None:
                candidate_strs.add(candidate_str)

        fuzzy_matches = top_fuzzy_matches(
            item=self._input_str,
            candidate_items=tuple(candidate_strs),
            max_matches=max_suggestions,
        )

        return tuple(scored_item.item_str for scored_item in fuzzy_matches)
