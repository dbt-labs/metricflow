from __future__ import annotations

import logging
import os
import pathlib
import re
from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoggerCall:
    """Represents a call to the logger like `logger.info(...)` in the code base."""

    file_path: str
    matching_text: str


def find_log_calls_that_dont_use_lazy_format(directory_to_check: pathlib.Path) -> Sequence[LoggerCall]:
    """Searches `.py` files in the directory for log calls that don't use `LazyFormat` via regex."""
    logger_calls = []
    excluded_directories = {"venv", ".venv", "__pycache__"}
    for root, dirs, files in os.walk(directory_to_check):
        dirs[:] = [d for d in dirs if d not in excluded_directories and not d.startswith(".")]
        logger.debug(LazyFormat("Processing root", root=root))

        for file in files:
            if not file.endswith(".py"):
                continue

            file_path = os.path.join(root, file)
            with open(file_path, encoding="utf-8") as f:
                logger.debug(LazyFormat("Checking file", file_path=file_path))
                file_contents = f.read()
                match = re.search(r"logger\..*?\.\(((?!LazyFormat).)*?\)", file_contents, flags=re.DOTALL)
                if match is not None:
                    logger_calls.append(
                        LoggerCall(
                            file_path=file_path,
                            matching_text=match.group(0),
                        )
                    )

    return logger_calls
