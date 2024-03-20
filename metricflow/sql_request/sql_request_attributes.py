from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlRequestId:
    """Identifies a request (i.e. a call to SqlClient.query() or SqlClient.execute()) to the SQL engine."""

    id_str: str

    def __repr__(self) -> str:  # noqa: D105
        return self.id_str
