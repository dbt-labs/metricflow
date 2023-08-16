from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from pydantic import ValidationError

from metricflow.sql_request.sql_request_attributes import SqlJsonTag, SqlRequestTagSet

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CombinedSqlTags:
    """Groups system and extra tags for simplicity."""

    system_tags: SqlRequestTagSet = field(default_factory=SqlRequestTagSet)
    extra_tag: SqlJsonTag = field(default_factory=SqlJsonTag)


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
    _EXTRA_TAG_PREFIX = "-- MF_EXTRA_TAGS: "

    @staticmethod
    def add_tag_metadata_as_comment(sql_statement: str, combined_tags: CombinedSqlTags) -> str:  # noqa: D
        if combined_tags.system_tags.tags:
            sql_statement = (
                sql_statement + "\n" + SqlStatementCommentMetadata._TAG_PREFIX + combined_tags.system_tags.json()
            )

        if combined_tags.extra_tag.json_dict:
            serialized_json: Optional[str] = None
            try:
                serialized_json = json.dumps(combined_tags.extra_tag.json_dict)
            except Exception:
                logger.exception(f"Not including extra tag that couldn't be serialized: {combined_tags.extra_tag}\n")

            if serialized_json:
                sql_statement = sql_statement + "\n" + SqlStatementCommentMetadata._EXTRA_TAG_PREFIX + serialized_json

        return sql_statement

    @staticmethod
    def parse_tag_metadata_in_comments(sql_statement: str) -> CombinedSqlTags:  # noqa: D
        tag_sets: List[SqlRequestTagSet] = []
        extra_tags: List[SqlJsonTag] = []
        for line in sql_statement.split("\n"):
            if line.startswith(SqlStatementCommentMetadata._TAG_PREFIX):
                try:
                    json_str = line[len(SqlStatementCommentMetadata._TAG_PREFIX) :]
                    tag_sets.append(SqlRequestTagSet.parse_raw(json_str))
                except ValidationError:
                    logger.exception(f"Unable to parse tag metadata from line: {line}")

            if line.startswith(SqlStatementCommentMetadata._EXTRA_TAG_PREFIX):
                try:
                    json_str = line[len(SqlStatementCommentMetadata._EXTRA_TAG_PREFIX) :]
                    extra_tags.append(SqlJsonTag(json.loads(json_str)))
                except ValidationError:
                    logger.exception(f"Unable to parse extra tag metadata from line: {line}")

        if len(tag_sets) > 1:
            logger.error(
                f"Got multiple tag sets from parsing comments:\n"
                f"{pformat_big_objects(tag_sets)}\n"
                f"Using the first one."
            )

        if len(extra_tags) > 1:
            logger.error(
                f"Got multiple extra tags from parsing comments:\n"
                f"{pformat_big_objects(extra_tags)}\n"
                f"Using the first one."
            )

        return CombinedSqlTags(
            system_tags=tag_sets[0] if len(tag_sets) > 0 else SqlRequestTagSet(),
            extra_tag=extra_tags[0] if len(extra_tags) > 0 else SqlJsonTag(),
        )
