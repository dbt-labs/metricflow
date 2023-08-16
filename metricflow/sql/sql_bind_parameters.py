from __future__ import annotations

import datetime
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow.assert_one_arg import assert_exactly_one_arg_set
from metricflow.sql.sql_column_type import SqlColumnType


@dataclass(frozen=True)
class SqlBindParameterValue(SerializableDataclass):
    """SqlColumnType has issues with serialization, so using this union-style type."""

    str_value: Optional[str] = None
    int_value: Optional[int] = None
    float_value: Optional[float] = None
    datetime_value: Optional[datetime.datetime] = None
    date_value: Optional[datetime.date] = None
    bool_value: Optional[bool] = None

    def __post_init__(self) -> None:  # noqa: D
        assert_exactly_one_arg_set(
            str_value=self.str_value,
            int_value=self.int_value,
            float_value=self.float_value,
            datetime_value=self.datetime_value,
            date_value=self.date_value,
            bool_value=self.bool_value,
        )

    @property
    def union_value(self) -> SqlColumnType:  # noqa: D
        if self.str_value is not None:
            return self.str_value
        elif self.int_value is not None:
            return self.int_value
        elif self.float_value is not None:
            return self.float_value
        elif self.datetime_value is not None:
            return self.datetime_value
        elif self.date_value is not None:
            return self.date_value
        elif self.bool_value is not None:
            return self.bool_value
        raise RuntimeError("No values are set - this should have been prevented by the post init")

    @staticmethod
    def create_from_sql_column_type(value: SqlColumnType) -> SqlBindParameterValue:
        """Convenience method for creating these values. Frowning on the use of isinstance()."""
        if isinstance(value, str):
            return SqlBindParameterValue(str_value=value)
        elif isinstance(value, int):
            return SqlBindParameterValue(int_value=value)
        elif isinstance(value, float):
            return SqlBindParameterValue(float_value=value)
        elif isinstance(value, datetime.datetime):
            return SqlBindParameterValue(datetime_value=value)
        elif isinstance(value, datetime.date):
            return SqlBindParameterValue(date_value=value)
        elif isinstance(value, bool):
            return SqlBindParameterValue(bool_value=value)

        raise RuntimeError(f"Unhandled type: {type(value)}")


@dataclass(frozen=True)
class SqlBindParameter(SerializableDataclass):  # noqa: D
    key: str
    value: SqlBindParameterValue


@dataclass(frozen=True)
class SqlBindParameters(SerializableDataclass):
    """Helps to build execution parameters during SQL query rendering.

    These can be used as per https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
    """

    # Using tuples for immutability as dicts are not.
    param_items: Tuple[SqlBindParameter, ...] = ()

    def combine(self, additional_params: SqlBindParameters) -> SqlBindParameters:
        """Create a new set of bind parameters that includes parameters from this and additional_params."""
        if len(self.param_items) == 0:
            return additional_params

        if len(additional_params.param_items) == 0:
            return self

        self_dict = {item.key: item.value for item in self.param_items}
        other_dict = {item.key: item.value for item in additional_params.param_items}

        for key, value in other_dict.items():
            if key in self_dict and self_dict[key] != value:
                raise RuntimeError(
                    f"Conflict with key {key} in combining parameters. "
                    f"Existing params: {self_dict} Additional params: {other_dict}"
                )
        new_items = list(self.param_items)
        included_keys = set(item.key for item in new_items)
        for item in additional_params.param_items:
            if item.key in included_keys:
                continue
            new_items.append(item)
            included_keys.add(item.key)

        return SqlBindParameters(param_items=tuple(new_items))

    @property
    def param_dict(self) -> OrderedDict[str, SqlColumnType]:
        """Useful for passing into SQLAlchemy / DB-API methods."""
        param_dict: OrderedDict[str, SqlColumnType] = OrderedDict()
        for item in self.param_items:
            param_dict[item.key] = item.value.union_value
        return param_dict

    @staticmethod
    def create_from_dict(param_dict: Mapping[str, SqlColumnType]) -> SqlBindParameters:  # noqa: D
        return SqlBindParameters(
            tuple(
                SqlBindParameter(key=key, value=SqlBindParameterValue.create_from_sql_column_type(value))
                for key, value in param_dict.items()
            )
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore  # noqa: D
        return isinstance(other, SqlBindParameters) and self.param_dict == other.param_dict
