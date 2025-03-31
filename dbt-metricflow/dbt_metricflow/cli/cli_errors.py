from __future__ import annotations


class ModelCreationException(Exception):
    """Exception to represent errors related to the building a model."""

    def __init__(self, msg: str = "") -> None:  # noqa: D107
        error_msg = "An error occurred when attempting to build the semantic model"
        if msg:
            error_msg += f"\n{msg}"
        super().__init__(error_msg)
