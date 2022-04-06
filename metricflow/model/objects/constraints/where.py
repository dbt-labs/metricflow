from __future__ import annotations

import logging
import re
from typing import List, Optional, Dict, Any

from moz_sql_parser import parse as moz_parse

from metricflow.errors.errors import ConstraintParseException
from metricflow.model.objects.utils import ParseableField, HashableBaseModel
from metricflow.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)

LITERAL_STR = "literal"
INTERVAL_LITERAL = "interval"


class WhereClauseConstraint(HashableBaseModel, ParseableField):
    """Contains a string that is a where clause"""

    where: str
    linkable_names: List[str]
    sql_params: SqlBindParameters

    def __init__(  # noqa: D
        self,
        where: str = "",
        linkable_names: Optional[List[str]] = None,
        sql_params: Optional[SqlBindParameters] = None,
        # sql params: user-originated sql params that need to be escaped in a dialect-specific way keys are the
        # name of the template value in the `where` string, values are the string to be escaped and
        # inserted into the where string (ie where = "%(1)s", sql_values = {"1": "cote d'ivoire"})
    ) -> None:
        where = where.strip("\n") if where else ""
        linkable_names = linkable_names or []
        if sql_params is None:
            sql_params = SqlBindParameters()
        super().__init__(
            where=where,
            linkable_names=linkable_names,
            sql_params=sql_params,
        )

    @staticmethod
    def parse(s: str) -> WhereClauseConstraint:
        """Parse a string into a WhereClauseConstraint

        We are assuming here that if we needed to parse a string, we wouldn't have bind parameters.
        Because if we had bind-parameters, the string would have existing structure, and we wouldn't need to parse it.
        """
        s = strip_where(s)

        where_str = f"WHERE {s}"
        # to piggyback on moz sql parser we need a SELECT statement
        # moz breaks the sql statement into clauses:
        # where_str = "WHERE is_instant" yields -> {'select': {'value': '_'}, 'from': '_', 'where': 'is_instant'}
        # where_str = "WHERE is_instant AND country = 'vanuatu' AND is_lux or ds < '2020-01-02'" yields ->
        # {'select': {'value': '_'}, 'from': '_', 'where': {'or': [{'and': ['is_instant', {'eq': ['country', {'literal': 'vanuatu'}]}, 'is_lux']}, {'lt': ['ds', {'literal': '2020-01-02'}]}]}}
        parsed = moz_parse(f"select _ from _ {where_str}")
        if "where" not in parsed:
            raise ConstraintParseException(parsed)

        where = parsed["where"]
        if isinstance(where, dict):
            return WhereClauseConstraint(
                where=s,
                linkable_names=constraint_dimension_names_from_dict(where),
                sql_params=SqlBindParameters(),
            )
        elif isinstance(where, str):
            return WhereClauseConstraint(
                where=s,
                linkable_names=[where.strip()],
                sql_params=SqlBindParameters(),
            )
        else:
            raise TypeError(f"where-clause is neither a dict nor a string. Unexpectedly it is a {type(where)}")

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}" f"(where={self.where}, linkable_names={self.linkable_names})"


def strip_where(s: str) -> str:
    """Removes WHERE from the beginning of the string, if present (regardless of case)"""
    # '^' tells the regex to only check the beginning of the string
    return re.sub("^where ", "", s, flags=re.IGNORECASE)


def constraint_dimension_names_from_dict(where: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: D
    if not len(where.keys()) == 1:
        raise ConstraintParseException(f"expected parsed constraint to contain exactly one key; got {where}")

    dims = []
    for key, clause in where.items():
        if key == LITERAL_STR or key == INTERVAL_LITERAL:
            continue
        dims += _get_dimensions_from_clause(clause)

    return dims


def constraint_values_from_dict(where: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: d
    values = []
    for key, clause in where.items():
        if key == LITERAL_STR:
            values.append(clause)
        elif isinstance(clause, dict):
            values += constraint_values_from_dict(clause)
        elif isinstance(clause, list):
            for item in clause:
                if isinstance(item, dict):
                    values += constraint_values_from_dict(item)

    return values


def _constraint_dimensions_from_list(list_clause: List[Any]) -> List[str]:  # type: ignore[misc] # noqa: D
    dims = []
    for clause in list_clause:
        dims += _get_dimensions_from_clause(clause)

    return dims


def _get_dimensions_from_clause(clause: Any) -> List[str]:  # type: ignore[misc] # noqa: D
    if clause is not None:
        if isinstance(clause, dict):
            return constraint_dimension_names_from_dict(clause)
        elif isinstance(clause, list):
            return _constraint_dimensions_from_list(clause)
        elif isinstance(clause, str):
            return [clause.strip()]

    return []
