from __future__ import annotations

from typing import List, Sequence, Tuple

from metricflow_semantics.model.semantics.linkable_element import LinkableElement
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec


class RenderedSpecTracker:
    """Mutable object that is used to record the specs that were rendered in a where filter.

    This is useful for constructing a WhereFilterSpec as it includes a list of specs required by the filter.
    """

    def __init__(self) -> None:  # noqa: D107
        self._rendered_specs_to_elements: List[Tuple[LinkableInstanceSpec, Sequence[LinkableElement]]] = []
        self._rendered_specs: list[LinkableInstanceSpec] = []

    def record_rendered_spec(self, spec: LinkableInstanceSpec) -> None:
        """Records a spec that was rendered in a where filter and can be retrieved later through rendered_specs()."""
        self._rendered_specs.append(spec)

    @property
    def rendered_specs(self) -> Sequence[LinkableInstanceSpec]:
        """Returns specs that were recorded by record_rendered_spec()."""
        return self._rendered_specs
