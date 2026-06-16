from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass

PATH_TO_FAST_FROZEN_DATACLASS_TEST_CLASSES_PY_FILE = Path(__file__)


@dataclass(frozen=True, order=True)
class Item:  # noqa: D101
    item_field_0: str


@dataclass(frozen=True, order=True)
class ItemGroup:  # noqa: D101
    item_group_field_0: Item
    item_group_field_1: Item
    item_group_field_2: Item
    item_group_field_3: Item
    item_group_field_4: Item
    item_group_field_5: Item
    item_group_field_6: Item
    item_group_field_7: Item
    item_group_field_8: Item
    item_group_field_9: Item


def create_group(prefix: str) -> ItemGroup:  # noqa: D103
    return ItemGroup(
        item_group_field_0=Item(item_field_0=prefix + "0"),
        item_group_field_1=Item(item_field_0=prefix + "1"),
        item_group_field_2=Item(item_field_0=prefix + "2"),
        item_group_field_3=Item(item_field_0=prefix + "3"),
        item_group_field_4=Item(item_field_0=prefix + "4"),
        item_group_field_5=Item(item_field_0=prefix + "5"),
        item_group_field_6=Item(item_field_0=prefix + "6"),
        item_group_field_7=Item(item_field_0=prefix + "7"),
        item_group_field_8=Item(item_field_0=prefix + "8"),
        item_group_field_9=Item(item_field_0=prefix + "9"),
    )


@fast_frozen_dataclass()
class FastItem:  # noqa: D101
    item_field_0: str


@fast_frozen_dataclass()
class FastItemGroup:  # noqa: D101
    item_group_field_0: FastItem
    item_group_field_1: FastItem
    item_group_field_2: FastItem
    item_group_field_3: FastItem
    item_group_field_4: FastItem
    item_group_field_5: FastItem
    item_group_field_6: FastItem
    item_group_field_7: FastItem
    item_group_field_8: FastItem
    item_group_field_9: FastItem


def create_fast_group(prefix: str) -> FastItemGroup:  # noqa: D103
    return FastItemGroup(
        item_group_field_0=FastItem(item_field_0=prefix + "0"),
        item_group_field_1=FastItem(item_field_0=prefix + "1"),
        item_group_field_2=FastItem(item_field_0=prefix + "2"),
        item_group_field_3=FastItem(item_field_0=prefix + "3"),
        item_group_field_4=FastItem(item_field_0=prefix + "4"),
        item_group_field_5=FastItem(item_field_0=prefix + "5"),
        item_group_field_6=FastItem(item_field_0=prefix + "6"),
        item_group_field_7=FastItem(item_field_0=prefix + "7"),
        item_group_field_8=FastItem(item_field_0=prefix + "8"),
        item_group_field_9=FastItem(item_field_0=prefix + "9"),
    )


left = create_group("left")
right = create_group("right")

item_group_set = {left, right}
item_group_dict = {left: None, right: None}

fast_left = create_fast_group("left")
fast_right = create_fast_group("right")

fast_item_group_set = {fast_left, fast_right}
fast_item_group_dict = {fast_left: None, fast_right: None}
