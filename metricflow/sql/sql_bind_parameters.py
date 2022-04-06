from __future__ import annotations

from collections import OrderedDict
from typing import Dict, Any

from metricflow.model.objects.utils import HashableBaseModel


class SqlBindParameters(HashableBaseModel):
    """Helps to build execution parameters during SQL query rendering.

    These can be used as per https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
    """

    param_dict: Dict[str, str] = OrderedDict()

    def update(self, additional_params: SqlBindParameters) -> None:
        """Add the parameters to this set, mutating it."""
        for key, value in additional_params.param_dict.items():
            if key in self.param_dict:
                raise RuntimeError(
                    f"Conflict with key {key} in merging parameters. "
                    f"Existing params: {self.param_dict} Additional params: {additional_params}"
                )
        self.param_dict.update(additional_params.param_dict)

    def __eq__(self, other: Any) -> bool:  # type: ignore  # noqa: D
        return isinstance(other, SqlBindParameters) and self.param_dict == other.param_dict
