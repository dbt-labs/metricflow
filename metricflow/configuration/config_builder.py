from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ConfigKey:
    """Dataclass to represent a yaml key."""

    key: str
    value: str = ""
    comment: Optional[str] = None
