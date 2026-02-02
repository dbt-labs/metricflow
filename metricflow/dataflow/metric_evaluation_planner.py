from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from functools import cached_property

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.dataflow.metric_evaluation_resolver import MetricEvaluation

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class MetricEvaluationGroup:
    metric_evaluations: AnyLengthTuple[MetricEvaluation]

    @staticmethod
    def create(metric_evaluations: Iterable[MetricEvaluation]) -> MetricEvaluationGroup:
        return MetricEvaluationGroup(metric_evaluations=tuple(metric_evaluations))


class MetricEvaluationPlanner:
    def __init__(self, metric_evaluations: Sequence[MetricEvaluation]) -> None:
        self._metric_evaluations = tuple(metric_evaluations)

    @cached_property
    def _level_to_evaluations(self) -> Mapping[int, Sequence[MetricEvaluation]]:
        level_to_evaluations: defaultdict[int, list[MetricEvaluation]] = defaultdict(list)
        for metric_evaluation in self._metric_evaluations:
            level_to_evaluations[metric_evaluation.metric_descriptor.evaluation_level].append(metric_evaluation)

        return level_to_evaluations

    def build_plan(self) -> None:
        levels = sorted(self._level_to_evaluations)

        for level in levels:
            pass

    def _handle_level(self, current_level: int, passed_evaluations: Sequence[MetricEvaluation]) -> None:
        evaluations = self._level_to_evaluations[current_level]

        # for evaluation in evaluations:
        # if max(evaluation.referenced_levels) > current_level

        raise NotImplementedError
