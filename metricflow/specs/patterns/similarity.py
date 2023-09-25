from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import rapidfuzz


@dataclass(frozen=True)
class ScoredItem:  # noqa: D
    item_str: str
    score: float


def top_fuzzy_matches(
    item: str,
    candidate_items: Sequence[str],
    max_suggestions: int = 6,
) -> Sequence[ScoredItem]:
    """Return the top items (by edit distance) in candidate_items that fuzzy matches the given item.

    Return scores from -1 -> 0 inclusive.
    """
    normalized_scored_items = []

    # Rank choices by edit distance score.
    # extract() returns a tuple like (name, score)
    top_ranked_suggestions = sorted(
        rapidfuzz.process.extract(
            # This scorer seems to return the best results.
            item,
            list(candidate_items),
            limit=max_suggestions,
            scorer=rapidfuzz.fuzz.token_set_ratio,
        ),
        # Put the highest scoring item at the top of the list.
        key=lambda x: x[1],
        reverse=True,
    )

    for fuzz_tuple in top_ranked_suggestions:
        value = fuzz_tuple[0]
        score = fuzz_tuple[1]

        # fuzz scores from 0..100 so normalize non-exact matches to to -1..0
        normalized_scored_items.append(ScoredItem(item_str=value, score=-(100.0 - score) / 100.0))

    return normalized_scored_items
