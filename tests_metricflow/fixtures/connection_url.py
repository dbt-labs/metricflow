from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple


@dataclass(frozen=True)
class UrlQueryField:
    """Field name / values specified in the query part of a URL."""

    field_name: str
    values: Tuple[str, ...]


@dataclass(frozen=True)
class SqlEngineConnectionParameterSet:
    """Describes how to connect to a SQL engine."""

    url_str: str
    dialect: str
    query_fields: Tuple[UrlQueryField, ...]
    driver: Optional[str]
    username: Optional[str]
    password: Optional[str]
    hostname: Optional[str]
    port: Optional[int]
    database: Optional[str]

    # Custom-handling for Databricks.
    http_path: Optional[str]

    @staticmethod
    def create_from_url(url_str: str) -> SqlEngineConnectionParameterSet:
        """The URL roughly follows the format used by SQLAlchemy.

        * This implementation is used to avoid having to specify SQLAlchemy as a dependency.
        * Databricks has a URL with a semicolon that separates an additional parameter. e.g.
          `databricks://host:port/database;http_path=a/b/c`. From @tlento: "Our original Databricks client was built
          before they added an officially supported SQLAlchemy client, so we used the JDBC connection URI.
          https://docs.databricks.com/en/integrations/jdbc/authentication.html"
        """
        url_separator = ";"
        url_split = url_str.split(url_separator)
        if len(url_split) > 2:
            raise ValueError(f"Expected at most 1 {repr(url_separator)} in {url_str}")

        parsed_url = urllib.parse.urlparse(url_split[0])
        url_extra = url_split[1] if len(url_split) > 1 else None

        dialect_driver = parsed_url.scheme.split("+")
        if len(dialect_driver) > 2:
            raise ValueError(f"Expected at most one + in {repr(parsed_url.scheme)}")
        dialect = dialect_driver[0]
        driver = dialect_driver[1] if len(dialect_driver) > 1 else None

        # redshift://../dbname -> /dbname -> dbname
        database = parsed_url.path.lstrip("/")

        query_fields = tuple(
            UrlQueryField(field_name, tuple(values))
            for field_name, values in urllib.parse.parse_qs(parsed_url.query).items()
        )

        http_path = None
        if url_extra is not None:
            field_name_value_separator = "="
            url_extra_split = url_extra.split(field_name_value_separator)
            if len(field_name_value_separator) == 2:
                field_name = url_extra_split[0]
                value = url_split[1]
                if field_name.lower() == "http_path":
                    http_path = value

        return SqlEngineConnectionParameterSet(
            url_str=url_str,
            dialect=dialect,
            driver=driver,
            username=parsed_url.username,
            password=parsed_url.password,
            hostname=parsed_url.hostname,
            port=parsed_url.port,
            database=database,
            query_fields=query_fields,
            http_path=http_path,
        )

    def get_query_field_values(self, field_name: str) -> Sequence[str]:
        """In the URL query, return the values for the field with the given name.

        Returns an empty sequence if the field name is not specified.
        """
        for field in self.query_fields:
            if field.field_name == field_name:
                return field.values
        return ()
