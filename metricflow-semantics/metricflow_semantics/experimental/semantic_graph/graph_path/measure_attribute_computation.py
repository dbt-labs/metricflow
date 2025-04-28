from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.references import (
    ElementReference,
    MeasureReference,
)
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity

from metricflow_semantics.model.semantics.linkable_element import SemanticModelJoinPath, SemanticModelJoinPathElement


@dataclass(frozen=True)
class MeasureAttributeComputation:
    measure_reference: Optional[MeasureReference] = None
    semantic_model_join_path: Optional[SemanticModelJoinPath] = None
    source_element_reference_for_attribute: Optional[ElementReference] = None
    time_grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def with_time_grain(self, time_grain: TimeGranularity) -> MeasureAttributeComputation:
        return MeasureAttributeComputation(
            measure_reference=self.measure_reference,
            semantic_model_join_path=self.semantic_model_join_path,
            source_element_reference_for_attribute=self.source_element_reference_for_attribute,
            time_grain=time_grain,
            date_part=self.date_part,
        )

    def with_date_part(self, date_part: DatePart) -> MeasureAttributeComputation:
        return MeasureAttributeComputation(
            measure_reference=self.measure_reference,
            semantic_model_join_path=self.semantic_model_join_path,
            source_element_reference_for_attribute=self.source_element_reference_for_attribute,
            time_grain=self.time_grain,
            date_part=date_part,
        )

    def with_additional_join_path_element(
        self, join_path_element: SemanticModelJoinPathElement
    ) -> MeasureAttributeComputation:
        # TODO: Fix before PR.
        assert self.semantic_model_join_path is not None

        return MeasureAttributeComputation(
            measure_reference=self.measure_reference,
            semantic_model_join_path=(
                self.semantic_model_join_path.with_additional_join_path_element(join_path_element)
            ),
            source_element_reference_for_attribute=self.source_element_reference_for_attribute,
            time_grain=self.time_grain,
            date_part=self.date_part,
        )
