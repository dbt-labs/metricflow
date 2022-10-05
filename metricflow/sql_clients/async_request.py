from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import pandas as pd
from pydantic import ValidationError

from metricflow.object_utils import pformat_big_objects
from metricflow.protocols.sql_client import SqlClient
from metricflow.protocols.sql_request import SqlRequestId, SqlRequestResult, SqlRequestTagSet
from metricflow.sql.sql_bind_parameters import SqlBindParameters


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


class SqlRequestExecutorThread(threading.Thread):
    """Thread that helps to execute a request to the SQL engine asynchronously."""

    def __init__(  # noqa: D
        self,
        sql_client: SqlClient,
        request_id: SqlRequestId,
        statement: str,
        bind_parameters: SqlBindParameters,
        user_tags: SqlRequestTagSet,
        is_query: bool = True,
    ) -> None:
        """Initializer.

        Args:
            sql_client: SQL client used to execute statements.
            request_id: The request ID associated with the statement.
            statement: The statement to execute.
            bind_parameters: The parameters to use for the statement.
            user_tags: Tags that should be associated with the request for the statement.
            is_query: Whether the request is for .query (returns data) or .execute (does not return data)
        """
        self._sql_client = sql_client
        self._request_id = request_id
        self._statement = statement
        self._bind_parameters = bind_parameters
        self._user_tags = user_tags
        self._result: Optional[SqlRequestResult] = None
        self._is_query = is_query
        super().__init__(name=f"Async Execute SQL Request ID: {request_id}", daemon=True)

    def run(self) -> None:  # noqa: D
        start_time = time.time()
        try:
            statement = SqlStatementCommentMetadata.add_tag_metadata_as_comment(
                self._statement, self._user_tags.add_request_id(self._request_id)
            )
            logger.info(f"Running {self._request_id}")
            if self._is_query:
                df = self._sql_client.query(statement, self._bind_parameters)
                self._result = SqlRequestResult(df=df)
            else:
                self._sql_client.execute(statement, self._bind_parameters)
                self._result = SqlRequestResult(df=pd.DataFrame())
            logger.info(f"Successfully executed {self._request_id} in {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.exception(
                f"Unsuccessfully executed {self._request_id} in {time.time() - start_time:.2f}s with exception:"
            )
            self._result = SqlRequestResult(exception=e)

    @property
    def result(self) -> SqlRequestResult:  # noqa: D
        assert self._result is not None, ".result() should only be called once the thread is finished running"
        return self._result

    @property
    def request_id(self) -> SqlRequestId:  # noqa: D
        return self._request_id
