from __future__ import annotations
import itertools
import logging
import pprint
import textwrap
from collections.abc import Mapping
from dataclasses import is_dataclass, fields
import datetime
from hashlib import sha1
from typing import Sequence, TypeVar, Tuple, Union

from dbt_semantic_interfaces.objects.base import HashableBaseModel

logger = logging.getLogger(__name__)


def is_hashable_base_model(obj):  # type:ignore # noqa: D
    return isinstance(obj, HashableBaseModel)


def _to_pretty_printable_object(obj):  # type: ignore
    """Convert the object that will look nicely when fed into the PrettyPrinter.

    Main change is that dataclasses will have a field with the class name. In Python 3.10, the pretty printer class will
    support dataclasses, so we can remove this once we're on 3.10. Also tried the prettyprint package with dataclasses,
    but that prints full names for the classes e.g. a.b.MyClass and it also always added line breaks, even if an object
    could fit on one line, so preferred to not use that.

    e.g.
    metricflow.specs.DimensionSpec(
        element_name='country',
        identifier_links=()
    ),

    Instead, the below will print something like:

    {'class': 'DimensionSpec',
     'element_name': 'country_latest',
     'identifier_links': ({'class': 'IdentifierSpec',
                           'element_name': 'listing',
                           'identifier_links': ()},)}
    """
    if obj is None:
        return None

    elif isinstance(obj, (str, int, float)):
        return obj

    elif isinstance(obj, (list, tuple)):
        result = []
        for item in obj:
            result.append(_to_pretty_printable_object(item))

        if isinstance(obj, list):
            return result
        elif isinstance(obj, tuple):
            return tuple(result)

        assert False

    elif isinstance(obj, Mapping):
        result = {}
        for key, value in obj.items():
            result[_to_pretty_printable_object(key)] = _to_pretty_printable_object(value)
        return result

    elif is_dataclass(obj):
        result = {"class": type(obj).__name__}

        for field in fields(obj):
            result[field.name] = _to_pretty_printable_object(getattr(obj, field.name))
        return result
    elif is_hashable_base_model(obj):
        result = {"class": type(obj).__name__}

        for field_name, value in obj.dict().items():
            result[field_name] = _to_pretty_printable_object(value)
        return result

    # Can't make it more pretty.
    return obj


def pretty_format(obj) -> str:  # type: ignore
    """Return the object as a string that looks pretty."""
    if isinstance(obj, str):
        return obj
    return pprint.pformat(_to_pretty_printable_object(obj), width=80, sort_dicts=False)


def pformat_big_objects(*args, **kwargs) -> str:  # type: ignore
    """Prints a series of objects with many fields in a pretty way.

    See _to_pretty_printable_object() for more context on this format. Looks like:

    measure_recipe:
    {'class': 'MeasureRecipe',
     'measure_node': ReadSqlSourceNode(node_id=rss_140),
     'required_local_linkable_specs': ({'class': 'DimensionSpec',
                                        'element_name': 'is_instant',
                                        'identifier_links': ()},),
     'join_linkable_instances_recipes': ()}

    """
    items = []
    for arg in args:
        items.append(pretty_format(arg))
    for key, value in kwargs.items():
        items.append(f"{key}:")
        items.append(textwrap.indent(pretty_format(value), prefix="    "))
    return "\n".join(items)


SequenceT = TypeVar("SequenceT")


def flatten_nested_sequence(sequence_of_sequences: Sequence[Sequence[SequenceT]]) -> Tuple[SequenceT, ...]:
    """Convert a nested sequence into a flattened tuple.

    e.g. ((1,2), (3,4)) -> (1, 2, 3, 4)
    """
    return tuple(itertools.chain.from_iterable(sequence_of_sequences))


def hash_items(items: Sequence[SqlColumnType]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()


# Supported SQL column types (not comprehensive).
SqlColumnType = Union[str, int, float, datetime.datetime, datetime.date, bool]
