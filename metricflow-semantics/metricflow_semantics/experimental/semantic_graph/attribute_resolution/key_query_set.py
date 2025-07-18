from __future__ import annotations

import logging
from collections import defaultdict
from functools import cached_property
from typing import Mapping, Optional, Sequence

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple, Pair
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId

logger = logging.getLogger(__name__)

DsiEntityKeyQuery = AnyLengthTuple[str]
ModelIdTuple = AnyLengthTuple[SemanticModelId]
KeyQueryTuple = AnyLengthTuple[DsiEntityKeyQuery]


class KeyQueryGrouper:
    def __init__(self) -> None:
        self._model_id_tuple_to_key_queries: dict[ModelIdTuple, list[DsiEntityKeyQuery]] = defaultdict(list)

    def add(self, key_query: DsiEntityKeyQuery, source_model_ids: ModelIdTuple) -> None:
        self._model_id_tuple_to_key_queries[source_model_ids].append(key_query)

    def group(self) -> KeyQueryGroup:
        return KeyQueryGroup(
            pairs_of_model_id_tuple_and_key_query_tuple=tuple(
                (model_id_tuple, tuple(key_queries))
                for model_id_tuple, key_queries in self._model_id_tuple_to_key_queries.items()
            )
        )


@fast_frozen_dataclass()
class KeyQueryGroup:
    pairs_of_model_id_tuple_and_key_query_tuple: AnyLengthTuple[
        Pair[
            AnyLengthTuple[SemanticModelId],
            AnyLengthTuple[DsiEntityKeyQuery],
        ]
    ] = ()

    @staticmethod
    def create(
        model_id_set_by_key_query: Mapping[DsiEntityKeyQuery, OrderedSet[SemanticModelId]],
    ) -> KeyQueryGroup:
        model_id_tuple_to_key_queries: dict[ModelIdTuple, list[DsiEntityKeyQuery]] = defaultdict(list)

        for key_query, model_id_set in model_id_set_by_key_query.items():
            model_id_tuple_to_key_queries[tuple(model_id_set)].append(key_query)

        return KeyQueryGroup(
            pairs_of_model_id_tuple_and_key_query_tuple=tuple(
                (model_id_tuple, tuple(key_queries))
                for model_id_tuple, key_queries in model_id_tuple_to_key_queries.items()
            ),
        )

    @staticmethod
    def intersection(key_query_groups: Sequence[KeyQueryGroup]) -> KeyQueryGroup:
        group_count = len(key_query_groups)
        if group_count == 0:
            return KeyQueryGroup()

        if group_count == 1:
            return key_query_groups[0]

        common_key_queries: Optional[set[DsiEntityKeyQuery]] = None
        for key_query_grouping in key_query_groups:
            if common_key_queries is None:
                common_key_queries = set(
                    key_query
                    for _, key_query_tuple in key_query_grouping.pairs_of_model_id_tuple_and_key_query_tuple
                    for key_query in key_query_tuple
                )
            else:
                common_key_queries.intersection_update(
                    set(
                        key_query
                        for _, key_query_tuple in key_query_grouping.pairs_of_model_id_tuple_and_key_query_tuple
                        for key_query in key_query_tuple
                    )
                )

        assert common_key_queries is not None

        model_id_set_by_key_query: dict[DsiEntityKeyQuery, MutableOrderedSet[SemanticModelId]] = defaultdict(
            MutableOrderedSet
        )

        for key_query_grouping in key_query_groups:
            for model_id_set, key_query_set in key_query_grouping.pairs_of_model_id_tuple_and_key_query_tuple:
                for key_query in key_query_set:
                    if key_query in common_key_queries:
                        model_id_set_by_key_query[key_query].update(model_id_set)

        return KeyQueryGroup.create(
            model_id_set_by_key_query=model_id_set_by_key_query,
        )

    def filter_by_key_name(self, key_name: str) -> KeyQueryGroup:
        pairs_with_matching_key_name = tuple(
            (model_id_tuple, tuple(key_query for key_query in key_query_tuple if key_query[-1] == key_name))
            for model_id_tuple, key_query_tuple in self.pairs_of_model_id_tuple_and_key_query_tuple
        )
        pairs_with_empty_key_queries_removed = tuple(pair for pair in pairs_with_matching_key_name if pair[1])
        return KeyQueryGroup(
            pairs_of_model_id_tuple_and_key_query_tuple=pairs_with_empty_key_queries_removed,
        )

    def items(self) -> Sequence[Pair[DsiEntityKeyQuery, AnyLengthTuple[SemanticModelId]]]:
        result_list: list[Pair[DsiEntityKeyQuery, AnyLengthTuple[SemanticModelId]]] = []

        for model_id_set, key_query_set in self.pairs_of_model_id_tuple_and_key_query_tuple:
            for key_query in key_query_set:
                result_list.append((key_query, model_id_set))
        return result_list

    @cached_property
    def key_names(self) -> FrozenOrderedSet[str]:
        return FrozenOrderedSet(
            key_query[-1]
            for _, key_query_tuple in self.pairs_of_model_id_tuple_and_key_query_tuple
            for key_query in key_query_tuple
        )

    def with_common_source_models(self, source_model_ids: AnyLengthTuple[SemanticModelId]) -> KeyQueryGroup:
        return KeyQueryGroup(
            pairs_of_model_id_tuple_and_key_query_tuple=tuple(
                (model_ids + source_model_ids, key_queries)
                for model_ids, key_queries in self.pairs_of_model_id_tuple_and_key_query_tuple
            )
        )
