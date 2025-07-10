from __future__ import annotations

import logging
from collections import defaultdict
from typing import Iterable, Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class DsiEntityKeyQuerySet:
    entity_key_queries: AnyLengthTuple[DsiEntityKeyQuery]

    @staticmethod
    def intersection(query_sets: Iterable[DsiEntityKeyQuerySet]) -> DsiEntityKeyQuerySet:
        common_dunder_name_elements: Optional[set[AnyLengthTuple[str]]] = None
        for query_set in query_sets:
            if common_dunder_name_elements is None:
                common_dunder_name_elements = set(
                    key_query.query_dunder_name_elements for key_query in query_set.entity_key_queries
                )
            else:
                common_dunder_name_elements.intersection_update(
                    set(key_query.query_dunder_name_elements for key_query in query_set.entity_key_queries)
                )

        if common_dunder_name_elements is None:
            return DsiEntityKeyQuerySet(
                entity_key_queries=(),
            )

        dunder_name_elements_to_model_ids: dict[AnyLengthTuple[str], MutableOrderedSet[SemanticModelId]] = defaultdict(
            MutableOrderedSet
        )

        for key_query_set in query_sets:
            for key_query in key_query_set.entity_key_queries:
                dunder_name_elements = key_query.query_dunder_name_elements
                if dunder_name_elements in common_dunder_name_elements:
                    dunder_name_elements_to_model_ids[dunder_name_elements].update(key_query.accessed_model_ids)

        return DsiEntityKeyQuerySet(
            entity_key_queries=tuple(
                DsiEntityKeyQuery(
                    accessed_model_ids=tuple(model_ids),
                    query_dunder_name_elements=dunder_name_elements,
                )
                for dunder_name_elements, model_ids in dunder_name_elements_to_model_ids.items()
            )
        )

    def filter_by_key_name(self, key_name: str) -> DsiEntityKeyQuerySet:
        return DsiEntityKeyQuerySet(
            entity_key_queries=tuple(
                key_query
                for key_query in self.entity_key_queries
                if key_query.query_dunder_name_elements[-1] == key_name
            ),
        )


@fast_frozen_dataclass()
class DsiEntityKeyQuery:
    accessed_model_ids: AnyLengthTuple[SemanticModelId]
    query_dunder_name_elements: AnyLengthTuple[str]
