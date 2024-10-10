from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import (
    SemanticModelReference,
)

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.query_param_implementations import SavedQueryParameter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SavedQueryDependencySet:
    """The dependencies of a saved query.

    The primary use case is to handle creation of the cache item associated with the saved query. The dependencies
    listed in this class must be up-to-date before the cache associated with the saved query can be created. Otherwise,
    running the export / creating the cache may create a cache item that is out-of-date / unusable.
    """

    # The semantic models that the saved query depends on.
    semantic_model_references: Tuple[SemanticModelReference, ...]


class SavedQueryDependencyResolver:
    """Resolves the dependencies of a saved query. Also see `SavedQueryDependencySet`."""

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D107
        self._semantic_manifest = semantic_manifest
        # TODO: There seems to be issues with an empty manifest, so doing this check for now.
        self._query_parser: Optional[MetricFlowQueryParser] = None
        if len(semantic_manifest.semantic_models) > 0:
            self._query_parser = MetricFlowQueryParser(SemanticManifestLookup(semantic_manifest))

    def _resolve_dependencies(self, saved_query_name: str) -> SavedQueryDependencySet:
        if self._query_parser is None:
            logger.warning(
                "Trying to resolve the dependencies of a saved query when the provided semantic manifest does not "
                "contain semantic models is unexpected. Returning an empty set."
            )
            return SavedQueryDependencySet(())

        parse_result = self._query_parser.parse_and_validate_saved_query(
            saved_query_parameter=SavedQueryParameter(saved_query_name),
            where_filters=None,
            limit=None,
            time_constraint_start=None,
            time_constraint_end=None,
            order_by_names=None,
            order_by_parameters=None,
        )

        return SavedQueryDependencySet(
            semantic_model_references=tuple(
                sorted(
                    parse_result.queried_semantic_models,
                    key=lambda reference: reference.semantic_model_name,
                )
            ),
        )

    def resolve_dependencies(self, saved_query_name: str) -> SavedQueryDependencySet:
        """Return the dependencies of the given saved query in the manifest."""
        try:
            return self._resolve_dependencies(saved_query_name)
        except Exception:
            logger.exception(
                f"Got an exception while getting the dependencies of saved-query {repr(saved_query_name)}. "
                f"All semantic models will be returned instead for safety."
            )
            return SavedQueryDependencySet(
                semantic_model_references=tuple(
                    sorted(
                        (semantic_model.reference for semantic_model in self._semantic_manifest.semantic_models),
                        key=lambda reference: reference.semantic_model_name,
                    )
                ),
            )
