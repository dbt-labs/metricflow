from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import rapidfuzz.fuzz
import rapidfuzz.process


@dataclass(frozen=True)
class ScoredItem:  # noqa: D
    item_str: str
    # fuzz scores from 0..100, and the higher the score, the better the match.
    score: float


def top_fuzzy_matches(
    item: str,
    candidate_items: Sequence[str],
    max_matches: int = 6,
) -> Sequence[ScoredItem]:
    """Return the top items (by edit distance) in candidate_items that fuzzy matches the given item.

    Return scores from -1 -> 0 inclusive.
    """
    scored_items = []

    # Rank choices by edit distance score.
    # extract() returns a tuple like (name, score)
    top_ranked_suggestions = sorted(
        rapidfuzz.process.extract(
            # This scorer seems to return the best results.
            item,
            list(candidate_items),
            limit=max_matches,
            scorer=rapidfuzz.fuzz.token_set_ratio,
        ),
        # Put the highest scoring item at the top of the list.
        key=lambda item_and_score_tuple: item_and_score_tuple[1],
        reverse=True,
    )

    for fuzz_tuple in top_ranked_suggestions:
        value = fuzz_tuple[0]
        score = fuzz_tuple[1]

        scored_items.append(ScoredItem(item_str=value, score=score))

    return scored_items
