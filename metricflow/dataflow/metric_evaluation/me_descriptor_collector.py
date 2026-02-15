from __future__ import annotations

from typing import Optional, Iterable, AbstractSet, Mapping

from metricflow.dataflow.metric_evaluation.me_elements import MetricDescriptor
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat


class MetricDescriptorCollector:
    def __init__(self) -> None:
        self._descriptor_to_input_descriptors: dict[MetricDescriptor, FrozenOrderedSet[MetricDescriptor]] = {}

    def add_descriptor(
        self, metric_descriptor: MetricDescriptor, input_descriptors: Optional[Iterable[MetricDescriptor]]
    ) -> None:
        if metric_descriptor in self._descriptor_to_input_descriptors:
            raise RuntimeError(LazyFormat("Descriptor already added", metric_descriptor=metric_descriptor))
        self._descriptor_to_input_descriptors[metric_descriptor] = (
            FrozenOrderedSet(input_descriptors) if input_descriptors is not None else FrozenOrderedSet()
        )

    @property
    def descriptors(self) -> Set[MetricDescriptor]:
        return self._descriptor_to_input_descriptors.keys()

    def get_input_descriptors(self, metric_descriptor: MetricDescriptor) -> OrderedSet[MetricDescriptor]:
        result = self._descriptor_to_input_descriptors.get(metric_descriptor)
        if result is None:
            raise ValueError(
                LazyFormat(
                    "Unknown metric descriptor",
                    metric_descriptor=metric_descriptor,
                    known_descriptors=self._descriptor_to_input_descriptors.keys(),
                )
            )
        return result

    # def contains_descriptor(self, metric_descriptor: MetricDescriptor) -> bool:
    #     return metric_descriptor in self._descriptor_to_input_descriptors

    @property
    def descriptor_to_input_descriptors(self) -> Mapping[MetricDescriptor, OrderedSet[MetricDescriptor]]:
        return self._descriptor_to_input_descriptors
