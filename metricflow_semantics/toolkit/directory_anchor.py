from __future__ import annotations

import logging
import pathlib
import traceback

logger = logging.getLogger(__name__)


class DirectoryAnchor:
    """Defines a directory inside the repo.

    Using this object allows you to avoid using hard-coded paths and instead use objects that will be handled properly
    during refactoring.
    """

    def __init__(self) -> None:
        """Initializer.

        The directory associated this anchor is where it's initialized.
        """
        stack = traceback.extract_stack()
        self._directory = pathlib.Path(stack[-2].filename).parent

    @property
    def directory(self) -> pathlib.Path:  # noqa: D102
        return self._directory
