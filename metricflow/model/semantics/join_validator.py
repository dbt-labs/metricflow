from __future__ import annotations

from dataclasses import dataclass

from metricflow.instances import DataSourceReference, DataSourceElementReference, InstanceSet
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.object_utils import pformat_big_objects
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.references import IdentifierReference


@dataclass(frozen=True)
class DataSourceIdentifierJoinType:
    """Describe a type of join between data sources where identifiers are of the listed types."""

    left_identifier_type: IdentifierType
    right_identifier_type: IdentifierType


class DataSourceJoinValidator:
    """Checks to see if a join between two data sources should be allowed."""

    # Valid joins are the non-fanout joines.
    _VALID_IDENTIFIER_JOINS = (
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.PRIMARY
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.UNIQUE
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.PRIMARY
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.UNIQUE
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.PRIMARY
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.UNIQUE
        ),
    )

    _INVALID_IDENTIFIER_JOINS = (
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.FOREIGN
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.FOREIGN
        ),
        DataSourceIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.FOREIGN
        ),
    )

    def __init__(self, data_source_semantics: DataSourceSemanticsAccessor) -> None:  # noqa: D
        self._data_source_semantics = data_source_semantics

    def is_valid_data_source_join(
        self,
        left_data_source_reference: DataSourceReference,
        right_data_source_reference: DataSourceReference,
        on_identifier_reference: IdentifierReference,
    ) -> bool:
        """Return true if we should allow a join with the given parameters to resolve a query."""
        left_identifier = self._data_source_semantics.get_identifier_in_data_source(
            DataSourceElementReference.create_from_references(left_data_source_reference, on_identifier_reference)
        )

        right_identifier = self._data_source_semantics.get_identifier_in_data_source(
            DataSourceElementReference.create_from_references(right_data_source_reference, on_identifier_reference)
        )
        if left_identifier is None:
            return False
        if right_identifier is None:
            return False

        join_type = DataSourceIdentifierJoinType(left_identifier.type, right_identifier.type)

        if join_type in DataSourceJoinValidator._VALID_IDENTIFIER_JOINS:
            return True
        elif join_type in DataSourceJoinValidator._INVALID_IDENTIFIER_JOINS:
            return False

        raise RuntimeError(f"Join type not handled: {join_type}")

    @staticmethod
    def _data_source_of_identifier_in_instance_set(
        instance_set: InstanceSet,
        identifier_reference: IdentifierReference,
    ) -> DataSourceReference:
        """Return the data source where the identifier was defined in the instance set."""
        matching_instances = []
        for identifier_instance in instance_set.identifier_instances:
            assert len(identifier_instance.defined_from) == 1
            if (
                len(identifier_instance.spec.identifier_links) == 0
                and identifier_instance.spec.reference == identifier_reference
            ):
                matching_instances.append(identifier_instance)

        assert len(matching_instances) == 1, (
            f"Not exactly 1 matching identifier instances found: {matching_instances} for {identifier_reference} in "
            f"{pformat_big_objects(instance_set)}"
        )
        return matching_instances[0].origin_data_source_reference.data_source_reference

    def is_valid_instance_set_join(
        self,
        left_instance_set: InstanceSet,
        right_instance_set: InstanceSet,
        on_identifier_reference: IdentifierReference,
    ) -> bool:
        """Return true if the instance sets can be joined using the given identifier."""
        return self.is_valid_data_source_join(
            left_data_source_reference=DataSourceJoinValidator._data_source_of_identifier_in_instance_set(
                instance_set=left_instance_set, identifier_reference=on_identifier_reference
            ),
            right_data_source_reference=DataSourceJoinValidator._data_source_of_identifier_in_instance_set(
                instance_set=right_instance_set,
                identifier_reference=on_identifier_reference,
            ),
            on_identifier_reference=on_identifier_reference,
        )
