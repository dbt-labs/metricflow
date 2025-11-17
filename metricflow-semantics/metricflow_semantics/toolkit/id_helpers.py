from __future__ import annotations

import random
import string


def mf_random_id(length: int = 8, excluded_characters: str = "gjpqy") -> str:
    """Generates an 8-digit random alphanumeric string.

    The default `excluded_characters` are characters that extend below the line for better visual appearance.
    """
    alphabet = string.ascii_lowercase + string.digits
    filtered_alphabet = tuple(x for x in alphabet if x not in excluded_characters)
    return "".join(random.choices(filtered_alphabet, k=length))
