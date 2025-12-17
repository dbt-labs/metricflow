from __future__ import annotations

from pathlib import Path
from typing import Sequence

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.pydantic_rule_set import PydanticSemanticManifestTransformRuleSet
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_json_file
from metricflow_semantics.toolkit.mf_type_aliases import Pair
from typing_extensions import override

from tests_metricflow.release_validation.manifest_setup.manifest_setup import ManifestSetup, ManifestSetupSource
from tests_metricflow.release_validation.manifest_transforms.modify_time_spine import ModifyTimeSpineTableRule
from tests_metricflow.release_validation.manifest_transforms.normalize_sql import NormalizeSqlRule
from tests_metricflow.table_snapshot.table_snapshots import (
    SqlTableColumnDefinition,
    SqlTableColumnType,
    SqlTableSnapshot,
)


class ExternalManifestSetupSource(ManifestSetupSource):
    """Provides setups from a directory containing external manifests (JSON-serialized).

    Example use case: "Here are a bunch of JSON-serialized manifests from customers. Check to see that a new release
    of MF doesn't break any of their saved queries."

    Since manifests from external sources may be authored using engine-specific SQL and rely on the existence of
    specific tables, several manifest transformations are required so that the manifest can be used with
    `DuckDbExplainTester`.

    In general, references to specific SQL tables and user-defined SQL must be replaced with similar dummy values.
    Since dummy values are used, the tests using the modified manifest won't be completely faithful, but can still
    capture some potential errors.

    Please see the associated transform rules for details.
    """

    def __init__(self, manifest_directory: Path) -> None:
        """Initializer.

        Args:
            manifest_directory: Directory containing `*.json` files that represent serialized manifests.
        """
        self._manifest_directory = manifest_directory
        self._dummy_table_name = "dummy_table"
        self._time_spine_table_name = "time_spine"

    @override
    def get_manifest_setups(self) -> Sequence[ManifestSetup]:
        setups = []
        for manifest_name, semantic_manifest in self._find_manifests():
            schema_name = manifest_name
            dummy_table = SqlTable(schema_name=schema_name, table_name=self._dummy_table_name)
            dummy_table_snapshot = SqlTableSnapshot(
                table_name=dummy_table.table_name,
                schema_name=dummy_table.schema_name,
                column_definitions=(SqlTableColumnDefinition(name="int_column", type=SqlTableColumnType.INT),),
                rows=(("1",),),
                file_path=None,
            )
            time_spine_table = SqlTable(schema_name=schema_name, table_name=self._time_spine_table_name)
            time_spine_table_column_names = self._get_time_spine_column_names(semantic_manifest)
            time_spine_table_snapshot = SqlTableSnapshot(
                table_name=time_spine_table.table_name,
                schema_name=schema_name,
                column_definitions=tuple(
                    SqlTableColumnDefinition(name=column_name, type=SqlTableColumnType.TIME)
                    for column_name in time_spine_table_column_names
                ),
                rows=(tuple("2020-01-01" for _ in time_spine_table_column_names),),
                file_path=None,
            )
            setups.append(
                ManifestSetup(
                    manifest_name=manifest_name,
                    semantic_manifest=semantic_manifest,
                    table_snapshots=(dummy_table_snapshot, time_spine_table_snapshot),
                )
            )
        return setups

    @staticmethod
    def _get_time_spine_column_names(semantic_manifest: PydanticSemanticManifest) -> Sequence[str]:
        column_names = set()
        for time_spine_table_configuration in semantic_manifest.project_configuration.time_spine_table_configurations:
            column_names.add(time_spine_table_configuration.column_name)

        for time_spine in semantic_manifest.project_configuration.time_spines:
            column_names.add(time_spine.primary_column.name)
            for custom_grain in time_spine.custom_granularities:
                column_names.add(custom_grain.column_name or custom_grain.name)

        return sorted(column_names)

    def _load_manifest(self, manifest_name: str, manifest_path: Path) -> PydanticSemanticManifest:
        semantic_manifest = mf_load_manifest_from_json_file(manifest_path)
        rule_set = PydanticSemanticManifestTransformRuleSet().all_rules

        schema_name = manifest_name
        primary_rules = (
            NormalizeSqlRule(SqlTable(schema_name=schema_name, table_name=self._dummy_table_name)),
            ModifyTimeSpineTableRule(schema_name=schema_name, table_name=self._time_spine_table_name),
        ) + tuple(rule_set[0])
        secondary_rules = tuple(rule_set[1])

        return PydanticSemanticManifestTransformer.transform(
            semantic_manifest,
            ordered_rule_sequences=(
                primary_rules,
                secondary_rules,
            ),
        )

    def _find_manifests(self) -> Sequence[Pair[str, PydanticSemanticManifest]]:
        """Return pairs that group the manifest name and the associated manifest."""
        name_and_manifest_pairs = []
        for manifest_path in self._manifest_directory.rglob("*.json"):
            if manifest_path.stat().st_size == 0:
                continue

            manifest_name = manifest_path.name.replace(".json", "")
            name_and_manifest_pairs.append(
                (
                    manifest_name,
                    self._load_manifest(manifest_name, manifest_path),
                )
            )
        return name_and_manifest_pairs
