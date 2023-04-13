from __future__ import annotations

from typing import List, Optional

from metricflow.dataflow.sql_table import SqlTable
from metricflow.model.objects.common import Metadata
from metricflow.model.objects.base import ModelWithMetadataParsing, HashableBaseModel
from metricflow.object_utils import ExtendedEnum


class MaterializationLocation(ExtendedEnum):
    """Possible locations for a materialized table"""

    DW = "dw"
    FAST_CACHE = "fast_cache"
    TABLEAU = "tableau"


class MaterializationFormat(ExtendedEnum):
    """Possible formats for a materialized table"""

    WIDE = "wide"


class MaterializationTableauParams(HashableBaseModel):
    """Describes the projects to write to in Tableau."""

    projects: List[str]


class MaterializationDestination(HashableBaseModel):
    """Describes where/how a materialized table should be written"""

    location: MaterializationLocation
    format: MaterializationFormat
    rollups: Optional[List[List[str]]]
    tableau_params: Optional[MaterializationTableauParams]


class Materialization(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a materialization"""

    name: str
    description: Optional[str]
    metrics: List[str]
    dimensions: List[str]
    destinations: Optional[List[MaterializationDestination]]
    destination_table: Optional[SqlTable]
    metadata: Optional[Metadata]
