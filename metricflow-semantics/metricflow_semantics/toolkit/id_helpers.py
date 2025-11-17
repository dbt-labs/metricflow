from __future__ import annotations

import datetime
import random
import string
from hashlib import sha1
from typing import Sequence, Union


def mf_random_id(length: int = 8, excluded_characters: str = "gjpqy") -> str:
    """Generates an 8-digit random alphanumeric string.

    The default `excluded_characters` are characters that extend below the line for better visual appearance.
    """
    alphabet = string.ascii_lowercase + string.digits
    filtered_alphabet = tuple(x for x in alphabet if x not in excluded_characters)
    return "".join(random.choices(filtered_alphabet, k=length))


def hash_items(items: Sequence[Union[str, int, float, datetime.datetime, datetime.date, bool]]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()
