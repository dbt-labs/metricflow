from __future__ import annotations

import logging
import re
from typing import List, Optional, Dict, Any

from mo_sql_parsing import parse as mo_parse

from metricflow.errors.errors import ConstraintParseException
from metricflow.model.objects.base import HashableBaseModel, PydanticCustomInputParser, PydanticParseableValueType
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)

LITERAL_STR = "literal"
INTERVAL_LITERAL = "interval"


class WhereClauseConstraint(PydanticCustomInputParser, HashableBaseModel):
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

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> WhereClauseConstraint:
        """Parses a WhereClauseConstraint from a constraint string found in a user-provided model specification

        User-provided constraint strings are SQL snippets conforming to the expectations of SQL WHERE clauses,
        and as such we parse them using our standard parse method below.
        """
        if isinstance(input, str):
            return WhereClauseConstraint.parse(input)
        else:
            raise ValueError(f"Expected input to be of type string, but got type {type(input)} with value: {input}")

    @staticmethod
    def parse(s: str) -> WhereClauseConstraint:
        """Parse a string into a WhereClauseConstraint

        We are assuming here that if we needed to parse a string, we wouldn't have bind parameters.
        Because if we had bind-parameters, the string would have existing structure, and we wouldn't need to parse it.

        Implements new functional syntax escape mechanism.
        """
        s = strip_where(s)

        escaped_sections = list(set(strip_delimited(s)))  # includes {{ }}

        excluded_names = set()
        escaped_linkable_names = []

        for escaped_section in escaped_sections:
            escaped_name = (
                escaped_section.replace("{{", "").replace("}}", "").strip()
            )  # removes {{ and }} and strips whitespace around
            new_name = StructuredLinkableSpecName.from_name(
                escaped_name
            ).qualified_name  # replace escaped_portion with qualified name, since qualified name is used for column names
            s = s.replace(escaped_section, new_name)

            excluded_names.add(new_name)
            escaped_linkable_names.append(escaped_name)

        where_str = f"WHERE {s}"
        # to piggyback on moz sql parser we need a SELECT statement
        # moz breaks the sql statement into clauses:
        # where_str = "WHERE is_instant" yields -> {'select': {'value': '_'}, 'from': '_', 'where': 'is_instant'}
        # where_str = "WHERE is_instant AND country = 'vanuatu' AND is_lux or ds < '2020-01-02'" yields ->
        # {'select': {'value': '_'}, 'from': '_', 'where': {'or': [{'and': ['is_instant', {'eq': ['country', {'literal': 'vanuatu'}]}, 'is_lux']}, {'lt': ['ds', {'literal': '2020-01-02'}]}]}}
        parsed = mo_parse(f"select _ from _ {where_str}")
        if "where" not in parsed:
            raise ConstraintParseException(parsed)

        where = parsed["where"]
        if isinstance(where, dict):
            if not len(where.keys()) == 1:
                raise ConstraintParseException(f"expected parsed constraint to contain exactly one key; got {where}")
            parsed_linkable_names = list(
                filter(lambda x: x not in excluded_names, constraint_dimension_names_from_dict(where))
            )
            return WhereClauseConstraint(
                where=s,
                linkable_names=escaped_linkable_names + parsed_linkable_names,
                sql_params=SqlBindParameters(),
            )
        elif isinstance(where, str):
            parsed_linkable_names = list(filter(lambda x: x not in excluded_names, [where.strip()]))
            return WhereClauseConstraint(
                where=s,
                linkable_names=escaped_linkable_names + parsed_linkable_names,
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


def strip_delimited(s: str) -> List[str]:
    """Finds all the portions of string that are escaped with the {{...}} syntax."""
    # the '\{\{' and '\}\}' parts match the {{ and }}, respectively. { and } need to be escaped in regex.
    # the .*? matches every possible character non-greedily, choosing the minimum match.
    pattern = r"\{\{.*?\}\}"
    return re.findall(pattern, s)


def constraint_dimension_names_from_dict(where: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: D
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
