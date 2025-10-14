from __future__ import annotations

import random
import string


def random_id() -> str:
    """Generates an 8-digit random alphanumeric string."""
    alphabet = string.ascii_lowercase + string.digits
    # Characters that go below the line are visually unappealing, so don't use those.
    filtered_alphabet = [x for x in alphabet if x not in "gjpqy"]
    return "".join(random.choices(filtered_alphabet, k=8))
