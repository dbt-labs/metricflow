from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

from metricflow.instances import DataSourceReference, DataSourceElementReference, IdentifierInstance, InstanceSet
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.object_utils import pformat_big_objects
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.references import IdentifierReference


@dataclass(frozen=True)
class DataSourceIdentifierJoinType:
    """Describe a type of join between data sources where identifiers are of the listed types."""

    left_identifier_type: IdentifierType
    right_identifier_type: IdentifierType


@dataclass(frozen=True)
class DataSourceIdentifierJoin:
    """Link between an identifier and a DataSourceIdentifierJoinType."""

    identifier_reference: IdentifierReference
    join_type: DataSourceIdentifierJoinType


@dataclass(frozen=True)
class DataSourceJoin:
    """Links between two data sources and the valid way to join them."""

    left_data_source_reference: DataSourceReference
    right_data_source_reference: DataSourceReference
    identifier_joins: List[DataSourceIdentifierJoin]


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

    def get_joinable_data_sources(
        self, left_data_source_reference: DataSourceReference, include_multi_hop: bool = False
    ) -> Dict[str, DataSourceJoin]:
        """List all data sources that can join to given data source, and the identifiers to join them (max 2-hop joins)."""
        data_source = self._data_source_semantics.get_by_reference(data_source_reference=left_data_source_reference)
        assert data_source is not None

        data_source_joins: Dict[str, DataSourceJoin] = {}
        for identifier in data_source.identifiers:
            # Verify single-hop joins
            identifier_reference = IdentifierReference(element_name=identifier.name)
            joinable_data_source_references = self._get_new_joinable_data_sources_for_identifier(
                left_data_source_reference=left_data_source_reference,
                known_data_source_joins=data_source_joins,
                identifier_reference=identifier_reference,
            )
            for (joinable_data_source_reference, join_type) in joinable_data_source_references:
                data_source_joins[joinable_data_source_reference.data_source_name] = DataSourceJoin(
                    left_data_source_reference=left_data_source_reference,
                    right_data_source_reference=joinable_data_source_reference,
                    identifier_joins=[
                        DataSourceIdentifierJoin(identifier_reference=identifier_reference, join_type=join_type)
                    ],
                )

                if not include_multi_hop:
                    continue

                # Verify multi-hop joins
                joinable_data_source = self._data_source_semantics.get_by_reference(
                    data_source_reference=joinable_data_source_reference
                )
                assert joinable_data_source is not None
                for secondary_identifier in joinable_data_source.identifiers:
                    secondary_identifier_reference = IdentifierReference(element_name=secondary_identifier.name)
                    secondary_joinable_data_source_references = self._get_new_joinable_data_sources_for_identifier(
                        left_data_source_reference=joinable_data_source_reference,
                        known_data_source_joins=data_source_joins,
                        identifier_reference=secondary_identifier_reference,
                    )
                    for (
                        secondary_joinable_data_source_reference,
                        secondary_join_type,
                    ) in secondary_joinable_data_source_references:
                        if (
                            secondary_joinable_data_source_reference.data_source_name
                            == left_data_source_reference.data_source_name
                        ):
                            continue
                        data_source_joins[secondary_joinable_data_source_reference.data_source_name] = DataSourceJoin(
                            left_data_source_reference=left_data_source_reference,
                            right_data_source_reference=secondary_joinable_data_source_reference,
                            identifier_joins=[
                                DataSourceIdentifierJoin(
                                    identifier_reference=identifier_reference, join_type=join_type
                                ),
                                DataSourceIdentifierJoin(
                                    identifier_reference=secondary_identifier_reference, join_type=secondary_join_type
                                ),
                            ],
                        )

        return data_source_joins

    def _get_new_joinable_data_sources_for_identifier(
        self,
        left_data_source_reference: DataSourceReference,
        known_data_source_joins: Dict[str, DataSourceJoin],
        identifier_reference: IdentifierReference,
    ) -> List[Tuple[DataSourceReference, DataSourceIdentifierJoinType]]:
        new_joinable_data_sources: List[Tuple[DataSourceReference, DataSourceIdentifierJoinType]] = []
        identifier_data_sources = self._data_source_semantics.get_data_sources_for_identifier(
            identifier_reference=identifier_reference
        )
        for identifier_data_source in identifier_data_sources:
            if (
                identifier_data_source.name == left_data_source_reference.data_source_name
                or identifier_data_source.name in known_data_source_joins
            ):
                continue

            right_data_source_reference = DataSourceReference(data_source_name=identifier_data_source.name)
            valid_join_type = self.get_valid_data_source_identifier_join_type(
                left_data_source_reference=left_data_source_reference,
                right_data_source_reference=right_data_source_reference,
                on_identifier_reference=identifier_reference,
            )
            if valid_join_type is not None:
                new_joinable_data_sources.append((right_data_source_reference, valid_join_type))

        return new_joinable_data_sources

    def get_valid_data_source_identifier_join_type(
        self,
        left_data_source_reference: DataSourceReference,
        right_data_source_reference: DataSourceReference,
        on_identifier_reference: IdentifierReference,
    ) -> Optional[DataSourceIdentifierJoinType]:
        """Get valid join type used to join data sources on given identifier, if exists."""
        left_identifier = self._data_source_semantics.get_identifier_in_data_source(
            DataSourceElementReference.create_from_references(left_data_source_reference, on_identifier_reference)
        )

        right_identifier = self._data_source_semantics.get_identifier_in_data_source(
            DataSourceElementReference.create_from_references(right_data_source_reference, on_identifier_reference)
        )
        if left_identifier is None or right_identifier is None:
            return None

        join_type = DataSourceIdentifierJoinType(left_identifier.type, right_identifier.type)

        if join_type in DataSourceJoinValidator._VALID_IDENTIFIER_JOINS:
            return join_type
        elif join_type in DataSourceJoinValidator._INVALID_IDENTIFIER_JOINS:
            return None

        raise RuntimeError(f"Join type not handled: {join_type}")

    def is_valid_data_source_join(
        self,
        left_data_source_reference: DataSourceReference,
        right_data_source_reference: DataSourceReference,
        on_identifier_reference: IdentifierReference,
    ) -> bool:
        """Return true if we should allow a join with the given parameters to resolve a query."""
        return (
            self.get_valid_data_source_identifier_join_type(
                left_data_source_reference=left_data_source_reference,
                right_data_source_reference=right_data_source_reference,
                on_identifier_reference=on_identifier_reference,
            )
            is not None
        )

    @staticmethod
    def _data_source_of_identifier_in_instance_set(
        instance_set: InstanceSet,
        identifier_reference: IdentifierReference,
    ) -> DataSourceReference:
        """Return the data source where the identifier was defined in the instance set."""
        matching_instances: List[IdentifierInstance] = []
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
