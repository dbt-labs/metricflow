from __future__ import annotations

import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import dateutil.parser
import yaml
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.base import FrozenBaseModel
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.id_helpers import mf_sha1_iterables

from metricflow.data_table.column_types import CellValue
from metricflow.data_table.mf_table import MetricFlowDataTable
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlTableSnapshotHash:
    """A hash that can be used to compare whether a SQL table snapshot has changed."""

    str_value: str

    @staticmethod
    def create_from_hashes(hashes: Sequence[SqlTableSnapshotHash]) -> SqlTableSnapshotHash:  # noqa: D102
        return SqlTableSnapshotHash(mf_sha1_iterables((one_hash.str_value for one_hash in hashes)))


class SqlTableColumnType(Enum):  # noqa: D101
    STRING = "STRING"
    TIME = "TIME"
    FLOAT = "FLOAT"
    INT = "INT"
    BOOLEAN = "BOOLEAN"


class SqlTableColumnDefinition(FrozenBaseModel):
    """Pydantic class to help parse column definitions in a table snapshots that are defined in YAML."""

    # Pydantic feature to throw errors on extra fields.
    class Config:  # noqa: D106
        extra = "forbid"

    name: str
    type: SqlTableColumnType


class SqlTableSnapshotTypeException(Exception):  # noqa: D101
    pass


DEFAULT_SCHEMA = "default_schema"


class SqlTableSnapshot(FrozenBaseModel):
    """Pydantic class to help parse table snapshots that are defined in YAML."""

    # Pydantic feature to throw errors on extra fields.
    class Config:  # noqa: D106
        extra = "forbid"

    table_name: str
    column_definitions: Tuple[SqlTableColumnDefinition, ...]
    rows: Tuple[Tuple[Optional[str], ...], ...]
    file_path: Optional[Path]
    schema_name: str

    @staticmethod
    def create(  # noqa: D102
        table_name: str,
        column_definitions: Iterable[SqlTableColumnDefinition],
        rows: Iterable[Iterable[Optional[str]]],
        file_path: Optional[Path] = None,
        schema_name: Optional[str] = None,
    ) -> SqlTableSnapshot:
        return SqlTableSnapshot(
            table_name=table_name,
            column_definitions=tuple(column_definitions),
            rows=tuple(tuple(row) for row in rows),
            file_path=file_path,
            schema_name=schema_name if schema_name is not None else "DEFAULT_SCHEMA",
        )

    def with_schema_name(self, schema_name: str) -> SqlTableSnapshot:  # noqa: D102
        return SqlTableSnapshot(
            table_name=self.table_name,
            column_definitions=self.column_definitions,
            rows=self.rows,
            file_path=self.file_path,
            schema_name=schema_name,
        )

    @property
    def snapshot_hash(self) -> SqlTableSnapshotHash:
        """Return a hash that can be used to summarize the schema and data of the snapshot."""
        return SqlTableSnapshotHash(
            mf_sha1_iterables(
                (self.table_name,),
                (column_definition.name for column_definition in self.column_definitions),
                (column_definition.type.name for column_definition in self.column_definitions),
                (cell or "" for row in self.rows for cell in row),
            )
        )

    @staticmethod
    def _parse_bool_str(bool_str: str) -> bool:
        if bool_str.lower() == "false":
            return False
        elif bool_str.lower() == "true":
            return True
        else:
            raise RuntimeError(f"Invalid string representation of a boolean: {bool_str}")

    @property
    def as_data_table(self) -> MetricFlowDataTable:
        """Return this snapshot as represented by an equivalent data_table."""
        # In the YAML files, all values are strings, but they need to be converted to defined type so that it can be
        # properly represented in a data_table

        type_converted_rows = []
        for row in self.rows:
            type_converted_row: List[CellValue] = []
            for column_num, column_value in enumerate(row):
                column_type = self.column_definitions[column_num].type
                if column_value is None:
                    type_converted_row.append(None)
                elif column_type is SqlTableColumnType.STRING:
                    type_converted_row.append(column_value)
                elif column_type is SqlTableColumnType.TIME:
                    type_converted_row.append(dateutil.parser.parse(column_value))
                elif column_type is SqlTableColumnType.INT:
                    type_converted_row.append(int(column_value))
                elif column_type is SqlTableColumnType.FLOAT:
                    type_converted_row.append(float(column_value))
                elif column_type is SqlTableColumnType.BOOLEAN:
                    type_converted_row.append(SqlTableSnapshot._parse_bool_str(column_value))
                else:
                    assert_values_exhausted(column_type)
            type_converted_rows.append(type_converted_row)

        return MetricFlowDataTable.create_from_rows(
            column_names=[column_definition.name for column_definition in self.column_definitions],
            rows=type_converted_rows,
        )


class SqlTableSnapshotLoader:
    """Loads a snapshot of a table into the SQL engine."""

    def __init__(self, ddl_sql_client: SqlClientWithDDLMethods) -> None:  # noqa: D107
        self._ddl_sql_client = ddl_sql_client

    def load(self, table_snapshot: SqlTableSnapshot) -> None:  # noqa: D102
        sql_table = SqlTable(schema_name=table_snapshot.schema_name, table_name=table_snapshot.table_name)

        self._ddl_sql_client.create_table_from_data_table(
            sql_table=sql_table,
            df=table_snapshot.as_data_table,
            # Without this set, the insert queries may be too long.
            chunk_size=500,
        )


class TableSnapshotException(Exception):  # noqa: D101
    pass


class TableSnapshotParseException(TableSnapshotException):  # noqa: D101
    pass


class SqlTableSnapshotRepository:
    """Stores table snapshots generated by parsing YAML files."""

    # The top level key in the YAML file
    TABLE_SNAPSHOT_DOCUMENT_KEY = "table_snapshot"

    def __init__(self, config_directory: Path) -> None:
        """Initializer.

        Args:
            config_directory: directory that should be searched for YAML files containing test cases.
        """
        self._config_directory = config_directory
        self._yaml_paths = SqlTableSnapshotRepository._find_all_yaml_file_paths(self._config_directory)
        self._table_snapshots: OrderedDict[str, SqlTableSnapshot] = OrderedDict()

        for file_path in self._yaml_paths:
            table_snapshots = SqlTableSnapshotRepository._parse_config_yaml(file_path)
            for table_snapshot in table_snapshots:
                if table_snapshot.table_name in self._table_snapshots:
                    raise ValueError(
                        f"Table with duplicate name found: {table_snapshot.table_name}. "
                        f"Conflicting paths: {table_snapshot.file_path} "
                        f"{self._table_snapshots[table_snapshot.table_name].file_path}"
                    )
                self._table_snapshots[table_snapshot.table_name] = table_snapshot

    @staticmethod
    def _parse_config_yaml(file_path: Path) -> Sequence[SqlTableSnapshot]:
        """Parse the YAML file at the given path into table snapshots."""
        results = []
        with open(file_path) as f:
            file_contents = f.read()
            for config_document in yaml.load_all(stream=file_contents, Loader=yaml.SafeLoader):
                # The config document can be None if there is nothing but white space between two `---`
                # this isn't really an issue, so lets just swallow it
                if config_document is None:
                    continue
                if not isinstance(config_document, dict):
                    raise TableSnapshotParseException(
                        f"Table snapshot YAML must be a dict. Got `{type(config_document)}`: {config_document}"
                    )

                keys = tuple(x for x in config_document.keys())
                if len(keys) != 1:
                    raise TableSnapshotParseException(
                        f"Table snapshot document should have one type of key, but this has {len(keys)}. "
                        f"Found keys: {keys} in {file_path}",
                    )

                # retrieve last top-level key as type
                document_type = next(iter(config_document.keys()))
                object_cfg = config_document[document_type]
                if document_type == SqlTableSnapshotRepository.TABLE_SNAPSHOT_DOCUMENT_KEY:
                    try:
                        results.append(SqlTableSnapshot.create(**object_cfg, file_path=Path(file_path)))
                    except Exception as e:
                        logger.exception(f"Error while parsing: {file_path}")
                        raise TableSnapshotParseException(f"Error while parsing: {file_path}") from e
                else:
                    raise TableSnapshotParseException(
                        f"Expected {SqlTableSnapshotRepository.TABLE_SNAPSHOT_DOCUMENT_KEY}, but got {document_type}"
                    )
        return results

    @staticmethod
    def _find_all_yaml_file_paths(directory: Path) -> Sequence[Path]:
        """Recursively search through the given directory for YAML files."""
        yaml_file_paths = []

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".yaml"):
                    yaml_file_paths.append(Path(root, file))

        return sorted(yaml_file_paths)

    @property
    def table_snapshots(self) -> Sequence[SqlTableSnapshot]:  # noqa: D102
        # tuple(self._table_snapshots.values()) shows a type warning
        return sorted(
            tuple(table_snapshot for table_snapshot in self._table_snapshots.values()),
            key=lambda table_snapshot: table_snapshot.table_name,
        )


TABLE_SNAPSHOT_REPOSITORY = SqlTableSnapshotRepository(
    Path(os.path.dirname(__file__)),
)
