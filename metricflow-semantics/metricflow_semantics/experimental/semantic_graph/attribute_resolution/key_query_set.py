from __future__ import annotations

import logging
from typing import Iterable, Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class DsiEntityKeyQuerySet:
    source_model_ids: FrozenOrderedSet[SemanticModelId]
    entity_key_queries: FrozenOrderedSet[EntityKeyQuery]

    @staticmethod
    def intersection(query_sets: Iterable[DsiEntityKeyQuerySet]) -> DsiEntityKeyQuerySet:
        result_set: Optional[DsiEntityKeyQuerySet] = None
        for query_set in query_sets:
            if result_set is None:
                result_set = query_set
            else:
                result_set = DsiEntityKeyQuerySet(
                    source_model_ids=result_set.source_model_ids.union(query_set.source_model_ids),
                    entity_key_queries=FrozenOrderedSet(
                        result_set.entity_key_queries.intersection(query_set.entity_key_queries)
                    ),
                )
        if result_set is None:
            return DsiEntityKeyQuerySet(
                source_model_ids=FrozenOrderedSet(),
                entity_key_queries=FrozenOrderedSet(),
            )

        return result_set

    def filter_by_key_name(self, key_name: str) -> DsiEntityKeyQuerySet:
        return DsiEntityKeyQuerySet(
            source_model_ids=self.source_model_ids,
            entity_key_queries=FrozenOrderedSet(
                key_query for key_query in self.entity_key_queries if key_query[-1] == key_name
            ),
        )


EntityKeyQuery = AnyLengthTuple[str]
