from __future__ import annotations

from enum import Enum
from typing import Any, List, Optional, Union

from metricflow_semantic_interfaces.implementations.base import HashableBaseModel
from msi_pydantic_shim import Field


class OSIDialect(str, Enum):
    """Supported SQL and expression language dialects."""

    ANSI_SQL = "ANSI_SQL"
    SNOWFLAKE = "SNOWFLAKE"
    MDX = "MDX"
    TABLEAU = "TABLEAU"
    DATABRICKS = "DATABRICKS"


class OSIAIContextObject(HashableBaseModel):
    """Structured AI context with instructions, synonyms, and examples."""

    instructions: Optional[str] = None
    synonyms: Optional[List[str]] = None
    examples: Optional[List[str]] = None

    class Config:  # noqa: D106
        extra = "allow"


OSIAIContext = Union[str, OSIAIContextObject]


class OSIVendor(str, Enum):
    """Vendors with supported custom extensions."""

    COMMON = "COMMON"
    SNOWFLAKE = "SNOWFLAKE"
    SALESFORCE = "SALESFORCE"
    DBT = "DBT"
    DATABRICKS = "DATABRICKS"


class OSICustomExtension(HashableBaseModel):
    """Vendor-specific metadata as a serialized JSON string."""

    vendor_name: OSIVendor
    data: str


class OSIDialectExpression(HashableBaseModel):
    """Expression in a specific dialect."""

    dialect: OSIDialect
    expression: str


class OSIExpression(HashableBaseModel):
    """Expression definition with multi-dialect support."""

    dialects: List[OSIDialectExpression]


class OSIDimension(HashableBaseModel):
    """Dimension metadata on a field."""

    is_time: bool


class OSIField(HashableBaseModel):
    """Row-level attribute for grouping, filtering, and metric expressions."""

    name: str
    expression: OSIExpression
    dimension: Optional[OSIDimension] = None
    label: Optional[str] = None
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[List[OSICustomExtension]] = None


class OSIDataset(HashableBaseModel):
    """Logical dataset representing a business entity (fact or dimension table)."""

    name: str
    source: str
    primary_key: Optional[List[str]] = None
    unique_keys: Optional[List[List[str]]] = None
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    fields: Optional[List[OSIField]] = None
    custom_extensions: Optional[List[OSICustomExtension]] = None


class OSIRelationship(HashableBaseModel):
    """Foreign key relationship between datasets."""

    name: str
    from_dataset: str = Field(..., alias="from")
    to: str
    from_columns: List[str]
    to_columns: List[str]
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[List[OSICustomExtension]] = None

    class Config:  # noqa: D106
        allow_population_by_field_name = True


class OSIMetric(HashableBaseModel):
    """Quantitative measure defined on business data."""

    name: str
    expression: OSIExpression
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[List[OSICustomExtension]] = None


class OSISemanticModel(HashableBaseModel):
    """Top-level container representing a complete semantic model."""

    name: str
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    datasets: List[OSIDataset]
    relationships: Optional[List[OSIRelationship]] = None
    metrics: Optional[List[OSIMetric]] = None
    custom_extensions: Optional[List[OSICustomExtension]] = None


class OSIDocument(HashableBaseModel):
    """Root OSI document."""

    version: str = "0.1.1"
    dialects: Optional[List[OSIDialect]] = None
    vendors: Optional[List[OSIVendor]] = None
    semantic_model: List[OSISemanticModel]

    def to_osi_json(self, **kwargs: Any) -> str:  # type: ignore[misc]
        """Serialize to OSI-compliant JSON (uses field aliases like 'from' and excludes None values)."""
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return self.json(**kwargs)
