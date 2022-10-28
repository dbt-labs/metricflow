from __future__ import annotations

import logging
from typing import Optional

from pydantic import ValidationError

from metricflow.object_utils import pformat_big_objects
from metricflow.protocols.sql_request import SqlRequestTagSet

logger = logging.getLogger(__name__)


class SqlStatementCommentMetadata:
    """Helps to add a comment to SQL statements to encode metadata (e.g. tags).

    Added at the end as some engines remove leading comments:
    https://docs.snowflake.com/en/release-notes/2017-04.html#queries-leading-comments-removed-during-execution

    Example:
        SELECT 1
    ->
        -- MF_REQUEST_METADATA: {"tag_dict": {"MF_REQUEST_ID": "mf_rid__tmhulwkt"}}
        SELECT 1
    """

    _TAG_PREFIX = "-- MF_REQUEST_METADATA: "

    @staticmethod
    def add_tag_metadata_as_comment(sql_statement: str, tag_set: SqlRequestTagSet) -> str:  # noqa: D
        if tag_set.tags:
            return sql_statement + "\n" + SqlStatementCommentMetadata._TAG_PREFIX + tag_set.json()
        else:
            return sql_statement

    @staticmethod
    def parse_tag_metadata_in_comments(sql_statement: str) -> Optional[SqlRequestTagSet]:  # noqa: D
        tag_sets = []
        for line in sql_statement.split("\n"):
            if line.startswith(SqlStatementCommentMetadata._TAG_PREFIX):
                try:
                    json_str = line[len(SqlStatementCommentMetadata._TAG_PREFIX) :]
                    tag_sets.append(SqlRequestTagSet.parse_raw(json_str))
                except ValidationError:
                    logger.exception(f"Unable to parse tag metadata from line: {line}")
        if len(tag_sets) > 1:
            logger.error(
                f"Got multiple tag sets from parsing comments:\n"
                f"{pformat_big_objects(tag_sets)}\n"
                f"Using the first one."
            )

        return tag_sets[0] if len(tag_sets) > 0 else None
