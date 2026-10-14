from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, FrozenSet, Generic, List, Optional, Sequence, Set, Tuple

from metricflow_semantic_interfaces.naming.keywords import DUNDER
from metricflow_semantic_interfaces.protocols import SemanticManifestT, SemanticModel
from metricflow_semantic_interfaces.references import SemanticModelReference
from metricflow_semantic_interfaces.type_enums import DimensionType, EntityType
from metricflow_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationRule,
    SemanticModelContext,
    ValidationError,
    ValidationIssue,
    validate_safely,
)

logger = logging.getLogger(__name__)

_LINKABLE_ENTITY_TYPES = (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)


@dataclass(frozen=True)
class _ResolvedDimension:
    """A hierarchy level resolved against the manifest.

    `entity_links` is empty for the anchor's own dimensions, however they are written (`city`, `store__city` or
    `branch__city`), so those compare equal. `semantic_model_names` are the semantic models that have the dimension,
    since a link can reach more than one.
    """

    entity_links: Tuple[str, ...]
    dimension_name: str
    is_time_dimension: bool
    semantic_model_names: FrozenSet[str]


class _DimensionResolver:
    """Resolves group-by names, as seen from one semantic model, to the dimensions they reach.

    A bare name (`city`) is a dimension of the anchor semantic model. A qualified name (`store__region`) starts at an
    entity of the anchor, and each further link (`store__region__country`) starts at an entity of a semantic model
    reached by the previous link. A link reaches every semantic model where that entity is primary, unique or natural,
    so a name may match a dimension in more than one of them.
    """

    def __init__(self, semantic_models: Sequence[SemanticModel]) -> None:  # noqa: D107
        self._models_by_linkable_entity: Dict[str, List[SemanticModel]] = defaultdict(list)
        for semantic_model in semantic_models:
            for entity in semantic_model.entities:
                if entity.type in _LINKABLE_ENTITY_TYPES:
                    self._models_by_linkable_entity[entity.name].append(semantic_model)

    def resolve(self, anchor: SemanticModel, name: str) -> Optional[_ResolvedDimension]:
        *entity_links, element_name = name.split(DUNDER)
        # The anchor's own dimensions are exposed through each of its linkable entities, so `branch__city` on a model
        # with a `branch` unique entity is the same dimension as `city`.
        if not entity_links or (
            len(entity_links) == 1
            and _exposes_locally(anchor, entity_links[0])
            and any(dimension.name == element_name for dimension in anchor.dimensions)
        ):
            return _resolved_dimension(entity_links=(), element_name=element_name, semantic_models=(anchor,))

        reachable_models: Sequence[SemanticModel] = (anchor,)
        for entity_link in entity_links:
            # A `primary_entity` shorthand has no entity column, so only a declared entity can join to other models.
            declares_entity = any(entity.name == entity_link for model in reachable_models for entity in model.entities)
            reachable_models = self._models_by_linkable_entity.get(entity_link, []) if declares_entity else []

        return _resolved_dimension(
            entity_links=tuple(entity_links), element_name=element_name, semantic_models=reachable_models
        )


def _resolved_dimension(
    entity_links: Tuple[str, ...], element_name: str, semantic_models: Sequence[SemanticModel]
) -> Optional[_ResolvedDimension]:
    matches = [
        (semantic_model.name, dimension.type)
        for semantic_model in semantic_models
        for dimension in semantic_model.dimensions
        if dimension.name == element_name
    ]
    if not matches:
        return None
    return _ResolvedDimension(
        entity_links=entity_links,
        dimension_name=element_name,
        is_time_dimension=any(dimension_type is DimensionType.TIME for _, dimension_type in matches),
        semantic_model_names=frozenset(semantic_model_name for semantic_model_name, _ in matches),
    )


def _exposes_locally(semantic_model: SemanticModel, entity_name: str) -> bool:
    return semantic_model.primary_entity == entity_name or any(
        entity.name == entity_name and entity.type in _LINKABLE_ENTITY_TYPES for entity in semantic_model.entities
    )


class SemanticModelHierarchiesRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that each hierarchy in a semantic model is a path through dimensions reachable from that model.

    * Hierarchy names are valid names, and unique within a semantic model.
    * A hierarchy has at least one level, and no dimension appears twice in it.
    * Every level, and every level's parent, resolves to a categorical dimension reachable from the semantic model.
      Time dimensions are left out because their drill-down is expressed by time granularities.
    * A level's parent is a different dimension of the same semantic model, reached through the same entity links as
      the level, so the parent value and the child value come from the same row.
    """

    @staticmethod
    @validate_safely(whats_being_done="checking that the hierarchies in semantic models resolve to dimensions")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        resolver = _DimensionResolver(semantic_manifest.semantic_models)
        issues: List[ValidationIssue] = []
        for semantic_model in semantic_manifest.semantic_models:
            issues.extend(
                SemanticModelHierarchiesRule._validate_semantic_model(semantic_model=semantic_model, resolver=resolver)
            )
        return issues

    @staticmethod
    def _resolve_level_dimension(
        resolver: _DimensionResolver,
        semantic_model: SemanticModel,
        hierarchy_name: str,
        name: str,
        subject: str,
        context: SemanticModelContext,
        issues: List[ValidationIssue],
    ) -> Optional[_ResolvedDimension]:
        resolved = resolver.resolve(anchor=semantic_model, name=name)
        if resolved is None:
            issues.append(
                ValidationError(
                    context=context,
                    message=(
                        f"{subject} in hierarchy `{hierarchy_name}` does not resolve to a dimension reachable from "
                        f"semantic model `{semantic_model.name}`."
                    ),
                )
            )
            return None
        if resolved.is_time_dimension:
            issues.append(
                ValidationError(
                    context=context,
                    message=(
                        f"{subject} in hierarchy `{hierarchy_name}` is a time dimension. Drilling down through time "
                        f"is expressed by time granularities, so time dimensions cannot be hierarchy levels."
                    ),
                )
            )
            return None
        return resolved

    @staticmethod
    @validate_safely(whats_being_done="checking the hierarchies of a semantic model")
    def _validate_semantic_model(
        semantic_model: SemanticModel, resolver: _DimensionResolver
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if not semantic_model.hierarchies:
            return issues

        context = SemanticModelContext(
            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
            semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
        )
        seen_hierarchy_names: Set[str] = set()

        for hierarchy in semantic_model.hierarchies:
            issues.extend(UniqueAndValidNameRule.check_valid_name(name=hierarchy.name, context=context))
            if hierarchy.name in seen_hierarchy_names:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"Semantic model `{semantic_model.name}` has more than one hierarchy named "
                            f"`{hierarchy.name}`."
                        ),
                    )
                )
            seen_hierarchy_names.add(hierarchy.name)

            if len(hierarchy.levels) == 0:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"Hierarchy `{hierarchy.name}` in semantic model `{semantic_model.name}` has no levels.",
                    )
                )

            seen_levels: Set[Tuple[Tuple[str, ...], str]] = set()
            for level in hierarchy.levels:
                resolved_level = SemanticModelHierarchiesRule._resolve_level_dimension(
                    resolver=resolver,
                    semantic_model=semantic_model,
                    hierarchy_name=hierarchy.name,
                    name=level.dimension,
                    subject=f"Level `{level.dimension}`",
                    context=context,
                    issues=issues,
                )
                if resolved_level is not None:
                    level_key = (resolved_level.entity_links, resolved_level.dimension_name)
                    if level_key in seen_levels:
                        issues.append(
                            ValidationError(
                                context=context,
                                message=(
                                    f"Hierarchy `{hierarchy.name}` in semantic model `{semantic_model.name}` lists the "
                                    f"level `{level.dimension}` more than once."
                                ),
                            )
                        )
                    seen_levels.add(level_key)

                if level.parent is None:
                    continue
                resolved_parent = SemanticModelHierarchiesRule._resolve_level_dimension(
                    resolver=resolver,
                    semantic_model=semantic_model,
                    hierarchy_name=hierarchy.name,
                    name=level.parent,
                    subject=f"Parent `{level.parent}` of level `{level.dimension}`",
                    context=context,
                    issues=issues,
                )
                if resolved_level is None or resolved_parent is None:
                    continue
                if resolved_parent.entity_links != resolved_level.entity_links or not (
                    resolved_parent.semantic_model_names & resolved_level.semantic_model_names
                ):
                    issues.append(
                        ValidationError(
                            context=context,
                            message=(
                                f"Parent `{level.parent}` of level `{level.dimension}` in hierarchy "
                                f"`{hierarchy.name}` does not come from the same rows as the level. The parent must "
                                f"hold each value's parent value in the same row, so both must be dimensions of one "
                                f"semantic model, reached through the same entity links."
                            ),
                        )
                    )
                elif resolved_parent.dimension_name == resolved_level.dimension_name:
                    issues.append(
                        ValidationError(
                            context=context,
                            message=(
                                f"Level `{level.dimension}` in hierarchy `{hierarchy.name}` names itself as its "
                                f"parent. The parent must be the dimension that holds each value's parent value."
                            ),
                        )
                    )

        return issues
