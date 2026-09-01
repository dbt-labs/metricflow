from __future__ import annotations

from typing import Dict, Optional

from metricflow.converters.models import OSIDataType
from metricflow_semantic_interfaces.type_enums import DataType

# Single source of truth for the OSI <-> MSI datatype mapping. The two enums are semantically
# equivalent 1:1 today; if Ossie's DataType enum changes (e.g. gains a new value), update here.
_OSI_TO_MSI_DATATYPE: Dict[OSIDataType, DataType] = {
    OSIDataType.STRING: DataType.STRING,
    OSIDataType.INTEGER: DataType.INTEGER,
    OSIDataType.DECIMAL: DataType.DECIMAL,
    OSIDataType.FLOAT: DataType.FLOAT,
    OSIDataType.BOOLEAN: DataType.BOOLEAN,
    OSIDataType.DATE: DataType.DATE,
    OSIDataType.TIME: DataType.TIME,
    OSIDataType.DATE_TIME: DataType.DATETIME,
    OSIDataType.DATE_TIME_TZ: DataType.DATETIME_TZ,
    OSIDataType.OPAQUE: DataType.OPAQUE,
}
_MSI_TO_OSI_DATATYPE: Dict[DataType, OSIDataType] = {msi: osi for osi, msi in _OSI_TO_MSI_DATATYPE.items()}


def osi_datatype_to_msi(datatype: Optional[OSIDataType]) -> Optional[DataType]:
    """Map an OSI `Field`/`Metric.datatype` value to its MSI `DataType` equivalent."""
    return _OSI_TO_MSI_DATATYPE.get(datatype) if datatype is not None else None


def msi_datatype_to_osi(datatype: Optional[DataType]) -> Optional[OSIDataType]:
    """Map an MSI `DataType` value to its OSI `Field`/`Metric.datatype` equivalent."""
    return _MSI_TO_OSI_DATATYPE.get(datatype) if datatype is not None else None
