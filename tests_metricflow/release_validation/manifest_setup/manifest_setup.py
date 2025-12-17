from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.pydantic_rule_set import PydanticSemanticManifestTransformRuleSet
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from typing_extensions import override

from tests_metricflow.release_validation.manifest_transforms.modify_model_node_relation import (
    ModifyModelNodeRelationRule,
)
from tests_metricflow.release_validation.manifest_transforms.modify_time_spine import ModifyTimeSpineTableRule
from tests_metricflow.table_snapshot.table_snapshots import (
    SqlTableSnapshot,
)

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class ManifestSetup:
    """Describes the setup for a manifest so that it can be used for running tests."""

    # The name of the manifest used for caching.
    manifest_name: str
    semantic_manifest: PydanticSemanticManifest
    # The tables that need to be available in the SQL engine for MF queries using the manifest to be valid.
    table_snapshots: AnyLengthTuple[SqlTableSnapshot]


class ManifestSetupSource(ABC):
    """Provides `ManifestSetup` instances.

    Subclasses can be used to handle different ways of finding manifests (e.g. recursively traverse a directory
    for serialized JSON manifest files).
    """

    @abstractmethod
    def get_manifest_setups(self) -> Sequence[ManifestSetup]:  # noqa: D102
        raise NotImplementedError


class InternalManifestSetupSource(ManifestSetupSource):
    """Source for manifests from the internal manifest test-set.

    Since the internal manifests use randomly generated schema names to provide isolation in shared SQL engines, using
    the manifest directly would result in generated SQL that is inconsistent from run to run.

    To generate consistent SQL, this transforms the given manifest and table snapshots to use a consistent schema name.
    For the transforms to work, table snapshots should have unique names.

    Alternatively, the '***' mask could be used instead.
    """

    def __init__(  # noqa: D107
        self,
        manifest_name: str,
        semantic_manifest: PydanticSemanticManifest,
        schema_name: str,
        table_snapshots: Sequence[SqlTableSnapshot],
    ) -> None:
        self._manifest_name = manifest_name
        self._semantic_manifest = semantic_manifest
        self._schema_name = schema_name
        self._table_snapshots = table_snapshots

        assert len(FrozenOrderedSet(table_snapshot.table_name for table_snapshot in self._table_snapshots)) == len(
            self._table_snapshots
        ), "Table names must be unique."

    @override
    def get_manifest_setups(self) -> Sequence[ManifestSetup]:
        rule_set = PydanticSemanticManifestTransformRuleSet().all_rules
        primary_rules = (
            ModifyModelNodeRelationRule(schema_name=self._schema_name),
            ModifyTimeSpineTableRule(schema_name=self._schema_name),
        ) + tuple(rule_set[0])
        secondary_rules = rule_set[1]
        semantic_manifest = PydanticSemanticManifestTransformer.transform(
            self._semantic_manifest,
            ordered_rule_sequences=(
                primary_rules,
                secondary_rules,
            ),
        )
        table_snapshots = tuple(
            table_snapshot.with_schema_name(self._schema_name) for table_snapshot in self._table_snapshots
        )
        return (
            ManifestSetup(
                manifest_name=self._manifest_name, semantic_manifest=semantic_manifest, table_snapshots=table_snapshots
            ),
        )
