# from __future__ import annotations
#
# from dataclasses import dataclass
# from typing import Optional, Tuple
#
# from dbt_semantic_interfaces.protocols import SemanticManifest
# from dbt_semantic_interfaces.references import SemanticModelReference
#
#
# @dataclass(frozen=True)
# class SavedQueryDependencySet:
#     """The dependencies of a saved query.
#
#     The primary use case is to handle creation of the cache item associated with the saved query. The dependencies
#     listed in this class must be up-to-date before the cache associated with the saved query can be created. Otherwise,
#     running the export / creating the cache may create a cache item that is out-of-date / unusable.
#     """
#
#     # A human-readable description for logging purposes.
#     description: Optional[str]
#     # The semantic models that the saved query depends on.
#     semantic_model_references: Tuple[SemanticModelReference, ...]
#
#
# @dataclass(frozen=True)
# class SavedQueryReference:
#     saved_query_name: str
#
#
# class SavedQueryDependencyResolver:
#     def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
#         self._semantic_manifest = semantic_manifest
#
#     def resolve_dependencies(self, saved_query_reference: SavedQueryReference) -> SavedQueryDependencySet:
#         return SavedQueryDependencySet(
#             description=(
#                 f"Dependencies for saved query {repr(saved_query_reference.saved_query_name)} include all semantic "
#                 f"models as a temporary result until the implementation is in place."
#             ),
#             semantic_model_references=tuple(
#                 semantic_model.reference for semantic_model in self._semantic_manifest.semantic_models
#             ),
#         )
