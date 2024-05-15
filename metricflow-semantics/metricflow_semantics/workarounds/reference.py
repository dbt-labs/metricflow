from __future__ import annotations

from typing import Iterable, Tuple

from dbt_semantic_interfaces.references import SemanticModelReference


def sorted_semantic_model_references(
    model_references: Iterable[SemanticModelReference],
) -> Tuple[SemanticModelReference, ...]:
    """Workaround until `*Reference` classes can be ordered in `dbt-semantic-interfaces`."""
    return tuple(sorted(model_references, key=lambda model_reference: model_reference.semantic_model_name))
