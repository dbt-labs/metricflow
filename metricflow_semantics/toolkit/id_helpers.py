from __future__ import annotations

import datetime
import random
import string
from hashlib import sha1
from typing import Iterable, Union


def mf_random_id(length: int = 8, excluded_characters: str = "gjpqy") -> str:
    """Generates an 8-digit random alphanumeric string.

    The default `excluded_characters` are characters that extend below the line for better visual appearance.
    """
    alphabet = string.ascii_lowercase + string.digits
    filtered_alphabet = tuple(x for x in alphabet if x not in excluded_characters)
    return "".join(random.choices(filtered_alphabet, k=length))


def mf_sha1_iterables(*iterables: Iterable[Union[str, int, float, datetime.datetime, datetime.date, bool]]) -> str:
    """Produces a SHA1 hash from any number of iterables."""
    hash_builder = sha1()
    for iterable in iterables:
        for item in iterable:
            hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()
