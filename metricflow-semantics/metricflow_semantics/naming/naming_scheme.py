from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

if TYPE_CHECKING:
    from metricflow_semantics.specs.instance_spec import InstanceSpec


class QueryItemNamingScheme(ABC):
    """Describes how to name items that are involved in a MetricFlow query.

    Most useful for group-by-items as there are different ways to name them like "user__country"
    or "TimeDimension('metric_time', 'DAY')".
    """

    @abstractmethod
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        """Following this scheme, return the string that can be used as an input that would specify the given spec.

        This is used to generate suggestions from available group-by-items if the user specifies a group-by-item that is
        invalid.

        If this scheme cannot accommodate the spec, return None. This is needed to handle unsupported cases in
        DunderNamingScheme, such as DatePart, but naming schemes should otherwise be complete.
        """
        pass

    @abstractmethod
    def spec_pattern(self, input_str: str, semantic_manifest_lookup: SemanticManifestLookup) -> SpecPattern:
        """Given an input that follows this scheme, return a spec pattern that matches the described input.

        This is used to generate suggestions from available group-by-items if the user specifies a group-by-item that is
        invalid.

        If this scheme cannot accommodate the spec, return None. This is needed to handle unsupported cases in
        DunderNamingScheme, such as DatePart, but naming schemes should otherwise be complete.
        """
        pass

    @abstractmethod
    def input_str_follows_scheme(self, input_str: str, semantic_manifest_lookup: SemanticManifestLookup) -> bool:
        """Returns true if the given input string follows this naming scheme.

        Consider adding a structured result that indicates why it does not match the scheme.
        """
        pass
