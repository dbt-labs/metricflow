from __future__ import annotations

import copy
import logging
from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint, SemanticManifestT
from metricflow_semantic_interfaces.transformations.pydantic_rule_set import (
    PydanticSemanticManifestTransformRuleSet,
)
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class SemanticManifestTransformer(Protocol[SemanticManifestT]):
    """Helps to make transformations to a model for convenience.

    Generally used to make it more convenient for the user to develop their model.
    """

    @abstractmethod
    def transform(
        self,
        model: SemanticManifestT,
        ordered_rule_sequences: Optional[Sequence[Sequence[SemanticManifestTransformRule]]] = None,
    ) -> SemanticManifestT:
        """Copies the passed in model, applies the rules to the new model, and then returns that model.

        It's important to note that some rules need to happen before or after other rules. Thus rules
        are passed in as an ordered tuple of rule sequences. Primary rules are run first, and then
        secondary rules. We don't currently have tertiary, quaternary, or etc currently, but this
        system easily allows for it.
        """
        pass


class PydanticSemanticManifestTransformer(ProtocolHint[SemanticManifestTransformer[PydanticSemanticManifest]]):
    """Transforms PydanticSemanticManifest."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformer[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform(  # noqa: D102
        model: PydanticSemanticManifest,
        ordered_rule_sequences: Optional[
            Sequence[Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]]
        ] = None,
    ) -> PydanticSemanticManifest:
        if ordered_rule_sequences is None:
            ordered_rule_sequences = PydanticSemanticManifestTransformRuleSet().all_rules

        model_copy = copy.deepcopy(model)

        for rule_sequence in ordered_rule_sequences:
            for rule in rule_sequence:
                model_copy = rule.transform_model(model_copy)

        return model_copy
