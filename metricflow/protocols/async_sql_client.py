from __future__ import annotations

from abc import abstractmethod
from typing import Protocol, Sequence, Optional, Callable

from metricflow.protocols.sql_client import SqlClient, SqlIsolationLevel
from metricflow.protocols.sql_request import SqlRequestId, SqlRequestResult, SqlJsonTag
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.async_request import CombinedSqlTags


class AsyncSqlClient(SqlClient, Protocol):
    """Defines methods for executing SQL statements asynchronously."""

    def async_query(
        self,
        statement: str,
        bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
        isolation_level: Optional[SqlIsolationLevel] = None,
    ) -> SqlRequestId:
        """Execute a query asynchronously."""
        raise NotImplementedError

    @abstractmethod
    def async_request_result(self, request_id: SqlRequestId) -> SqlRequestResult:
        """Wait until a async query has finished, and then return the result"""
        raise NotImplementedError

    @abstractmethod
    def async_execute(
        self,
        statement: str,
        bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
        isolation_level: Optional[SqlIsolationLevel] = None,
    ) -> SqlRequestId:
        """Execute a statement that does not return values asynchronously."""
        raise NotImplementedError

    @abstractmethod
    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:
        """Make a best-effort at canceling requests with tags that match the supplied function.

        The function arguments are the tags associated with the query, and should return a bool indicating whether
        the given query should be cancelled. Returns the number of cancellation commands sent.
        """
        raise NotImplementedError

    @abstractmethod
    def active_requests(self) -> Sequence[SqlRequestId]:
        """Return requests that are still in progress.

        If the results for a request have not yet been fetched with async_request_result(), it's considered in progress.
        """
        raise NotImplementedError
