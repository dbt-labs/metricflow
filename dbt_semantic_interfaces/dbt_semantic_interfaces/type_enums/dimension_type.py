from dbt_semantic_interfaces.enum_extension import ExtendedEnum


class DimensionType(ExtendedEnum):
    """Determines types of values expected of dimensions."""

    CATEGORICAL = "categorical"
    TIME = "time"

    def is_time_type(self) -> bool:
        """Checks if this type of dimension is a time type."""
        return self in [DimensionType.TIME]
