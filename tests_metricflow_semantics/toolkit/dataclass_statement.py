"""Methods used to generate dataclasses statements used in `fast_frozen_dataclass` tests."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantics.toolkit.string_helpers import mf_dedent, mf_newline_join


class DataclassType(Enum):
    """The type of dataclass definition to generate."""

    FROZEN = "frozen"
    FAST_FROZEN = "fast_frozen"


@dataclass(frozen=True)
class DataclassDefinition:
    """Statements that can be used with `timeit` to define a dataclass."""

    import_statement: str
    dataclass_statement: str


def _generate_item_dataclass_definition(
    dataclass_type: DataclassType,
    field_count: int,
) -> DataclassDefinition:
    """Generate a dataclass definition for testing performance of different types of dataclasses.

    Example result:
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class Item:
            item_field_0: str

        @dataclass(frozen=True)
        class ItemGroup:
            item_group_field_0: Item
            item_group_field_1: Item
            item_group_field_2: Item
            ...

        def create_group(prefix: str) -> ItemGroup:
            return ItemGroup(
                item_group_field_0=Item(item_field_0=prefix + '0'),
                item_group_field_1=Item(item_field_0=prefix + '1'),
                item_group_field_2=Item(item_field_0=prefix + '2'),
                ...
            )
    """
    if dataclass_type is DataclassType.FROZEN:
        import_statement = "from dataclasses import dataclass"
        decorator = "@dataclass(frozen=True, order=True)"
    elif dataclass_type is DataclassType.FAST_FROZEN:
        import_statement = "from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass"
        decorator = "@fast_frozen_dataclass()"
    else:
        assert_values_exhausted(dataclass_type)

    statement_items = [
        "",
        decorator,
        mf_dedent(
            """
            class Item:  # noqa: D101
                item_field_0: str
            """
        ),
        "",
        decorator,
        "class ItemGroup:  # noqa: D101",
    ]

    for i in range(field_count):
        statement_items.append(f"  item_group_field_{i}: Item")

    statement_items.append("")
    statement_items.append("def create_group(prefix: str) -> ItemGroup:  # noqa: D102")
    statement_items.append("    return ItemGroup(")
    for i in range(field_count):
        statement_items.append(f"        item_group_field_{i}=Item(item_field_0=prefix + '{i}'),")
    statement_items.append("    )")

    return DataclassDefinition(
        import_statement=import_statement,
        dataclass_statement="\n".join(statement_items),
    )


def _make_setup_statement(dataclass_type: DataclassType, field_count: int) -> str:
    """Create statements to generate dataclass definitions.

    Not used in test code path - used when generating code.

    Example result:
        <dataclass definitions>

        left = create_group('left')
        right = create_group('right')

        item_group_set = {left, right}
        item_group_dict = {left: None, right: None}
    """
    dataclass_definition = _generate_item_dataclass_definition(dataclass_type, field_count)

    return mf_newline_join(
        dataclass_definition.import_statement,
        dataclass_definition.dataclass_statement,
        "left = create_group('left')",
        "right = create_group('right')",
        "",
        "item_group_set = {left, right}",
        "item_group_dict = {left: None, right: None}",
    )
