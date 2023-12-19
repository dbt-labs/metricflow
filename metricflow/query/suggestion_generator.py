from __future__ import annotations

import logging
from typing import Sequence, Tuple

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.query.similarity import top_fuzzy_matches
from metricflow.specs.patterns.base_time_grain import BaseTimeGrainPattern
from metricflow.specs.patterns.none_date_part import NoneDatePartPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import InstanceSpec

logger = logging.getLogger(__name__)


class QueryItemSuggestionGenerator:
    """Returns specs that partially match a spec pattern created from user input. Used for suggestions in errors.

    Since suggestions are needed for group-by-items specified in the query and in where filters, an optional candidate
    filter can be specified to limit suggestions to the ones valid for the entire query. For use with where filters,
    a candidate filter is not needed as any available spec at a resolution node can be used.
    """

    # Adding these filters so that we don't get multiple suggestions that are similar, but with different
    # grains. Some additional thought is needed to tweak this as the base grain may not be the best suggestion.
    GROUP_BY_ITEM_CANDIDATE_FILTERS: Tuple[SpecPattern, ...] = (BaseTimeGrainPattern(), NoneDatePartPattern())

    def __init__(  # noqa: D
        self, input_naming_scheme: QueryItemNamingScheme, input_str: str, candidate_filters: Sequence[SpecPattern]
    ) -> None:
        self._input_naming_scheme = input_naming_scheme
        self._input_str = input_str
        self._candidate_filters = candidate_filters

    def input_suggestions(
        self,
        candidate_specs: Sequence[InstanceSpec],
        max_suggestions: int = 6,
    ) -> Sequence[str]:
        """Return the best specs that match the given pattern from candidate_specs and match the candidate_filer."""
        for candidate_filter in self._candidate_filters:
            candidate_specs = candidate_filter.match(candidate_specs)

        # Use edit distance to figure out the closest matches, so convert the specs to strings.
        candidate_strs = []
        for candidate_spec in candidate_specs:
            candidate_str = self._input_naming_scheme.input_str(candidate_spec)

            if candidate_str is not None:
                candidate_strs.append(candidate_str)

        fuzzy_matches = top_fuzzy_matches(
            item=self._input_str,
            candidate_items=tuple(candidate_strs),
            max_matches=max_suggestions,
        )

        return tuple(scored_item.item_str for scored_item in fuzzy_matches)
