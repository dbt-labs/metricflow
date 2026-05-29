from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.query_spec import InputSpecOrder
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.sql.sql_plan import SqlSelectColumn

logger = logging.getLogger(__name__)


class OutputColumnOrderer(ABC):
    """Defines how output columns should be ordered in a rendered `SELECT` statement."""

    @abstractmethod
    def order_columns(
        self, spec_to_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]]
    ) -> Sequence[SqlSelectColumn]:
        """Return select columns in output order for the given spec-to-columns mapping."""
        pass


class TypeGroupedOrderer(OutputColumnOrderer):
    """Return columns grouped by spec type.

    The order of the groups follows a sequence data consumers typically prefer.
    """

    @override
    def order_columns(
        self, spec_to_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]]
    ) -> Sequence[SqlSelectColumn]:
        return tuple(
            itertools.chain(
                *(spec_to_columns_mapping[spec] for spec in _specs_in_type_group_order(tuple(spec_to_columns_mapping))),
            )
        )


def _specs_in_input_order(input_spec_order: InputSpecOrder) -> tuple[InstanceSpec, ...]:
    """Return all specs from the input order in the order they were provided."""
    return input_spec_order.group_by_item_specs + input_spec_order.metric_specs


def _specs_in_type_group_order(specs: Sequence[InstanceSpec]) -> tuple[InstanceSpec, ...]:
    """Return specs grouped by type while preserving the order within each type group.

    `InstanceSpecSet.create_from_specs()` preserves the incoming iteration order for each grouped tuple, so the
    returned order is stable within each type.
    """
    grouped_specs = InstanceSpecSet.create_from_specs(specs)
    return tuple(
        itertools.chain(
            grouped_specs.time_dimension_specs,
            grouped_specs.entity_specs,
            grouped_specs.dimension_specs,
            grouped_specs.group_by_metric_specs,
            grouped_specs.metric_specs,
            grouped_specs.simple_metric_input_specs,
            grouped_specs.metadata_specs,
        )
    )


@dataclass(frozen=True)
class _InputSpecOrderValidation:
    """Captures discrepancies between the expected input order and the available specs."""

    unknown_specs: OrderedSet[InstanceSpec]
    unaccounted_specs: OrderedSet[InstanceSpec]

    @property
    def has_issues(self) -> bool:
        """Return whether the validation found any mismatch."""
        return bool(self.unknown_specs or self.unaccounted_specs)


def _validate_input_spec_order(
    input_spec_order: InputSpecOrder,
    spec_to_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]],
) -> _InputSpecOrderValidation:
    """Validate that the input order exactly describes the available output specs.

    The validation flags:
    * specs requested in the input order but missing from `spec_to_columns_mapping`,
    * specs available in `spec_to_columns_mapping` but omitted from the input order,
    * duplicate specs in the input order, which would otherwise duplicate output columns.
    """
    accounted_specs: MutableOrderedSet[InstanceSpec] = MutableOrderedSet()
    unknown_specs: MutableOrderedSet[InstanceSpec] = MutableOrderedSet()

    specs_in_input = _specs_in_input_order(input_spec_order)
    for spec in specs_in_input:
        if spec not in spec_to_columns_mapping:
            unknown_specs.add(spec)
            continue

        accounted_specs.add(spec)

    unaccounted_specs: MutableOrderedSet[InstanceSpec] = MutableOrderedSet()
    if len(accounted_specs) != len(spec_to_columns_mapping):
        for spec in spec_to_columns_mapping:
            if spec not in accounted_specs:
                unaccounted_specs.add(spec)

    return _InputSpecOrderValidation(
        unknown_specs=unknown_specs,
        unaccounted_specs=unaccounted_specs,
    )


def _log_input_spec_order_mismatch(
    input_spec_order: InputSpecOrder,
    spec_to_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]],
    validation: _InputSpecOrderValidation,
) -> None:
    """Log a detailed error describing why the input order could not be applied."""
    logger.error(
        LazyFormat(
            "The input spec order does not exactly describe `spec_to_columns_mapping`. This is a bug and should be"
            " investigated, but returning columns in the default grouped order to reduce user-facing errors.",
            unaccounted_specs=validation.unaccounted_specs,
            unknown_specs=validation.unknown_specs,
            input_spec_order=input_spec_order,
            spec_to_columns_mapping=spec_to_columns_mapping,
        )
    )


class InputOrderPreservingTypeGroupedOrderer(OutputColumnOrderer):
    """Group columns by spec type while preserving the relative order from the query input within each group."""

    def __init__(self, input_spec_order: InputSpecOrder) -> None:  # noqa: D107
        self._input_spec_order = input_spec_order

    @override
    def order_columns(
        self, spec_to_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]]
    ) -> Sequence[SqlSelectColumn]:
        validation = _validate_input_spec_order(self._input_spec_order, spec_to_columns_mapping)
        if validation.has_issues:
            _log_input_spec_order_mismatch(self._input_spec_order, spec_to_columns_mapping, validation)
            return TypeGroupedOrderer().order_columns(spec_to_columns_mapping)

        ordered_specs = _specs_in_input_order(self._input_spec_order)
        spec_to_group_order: dict[InstanceSpec, int] = {}

        for index, spec in enumerate(ordered_specs):
            spec_to_group_order[spec] = index

        def _group_sort_key(spec: InstanceSpec) -> int:
            return spec_to_group_order[spec]

        return tuple(
            itertools.chain(
                *(
                    spec_to_columns_mapping[spec]
                    for spec in sorted(_specs_in_type_group_order(ordered_specs), key=_group_sort_key)
                ),
            )
        )


class InputOrderPreservingOrderer(OutputColumnOrderer):
    """Return columns in the exact order that specs were provided in the query input."""

    def __init__(self, input_spec_order: InputSpecOrder) -> None:  # noqa: D107
        self._input_spec_order = input_spec_order

    @override
    def order_columns(
        self, spec_to_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]]
    ) -> Sequence[SqlSelectColumn]:
        validation = _validate_input_spec_order(self._input_spec_order, spec_to_columns_mapping)
        if validation.has_issues:
            _log_input_spec_order_mismatch(self._input_spec_order, spec_to_columns_mapping, validation)
            return TypeGroupedOrderer().order_columns(spec_to_columns_mapping)

        return tuple(
            itertools.chain(
                *(spec_to_columns_mapping[spec] for spec in _specs_in_input_order(self._input_spec_order)),
            )
        )
