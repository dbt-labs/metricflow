from __future__ import annotations

import importlib.util
import logging
import threading
from types import ModuleType
from typing import Mapping, Optional, Sequence, Union

import tabulate

from metricflow.data_table.column_types import CellValue

logger = logging.getLogger(__name__)


class _IsolatedTabulateRunner:
    """Helps run `tabulate` with different module options.

    The `tabulate.tabulate` method uses some options defined in the module instead of being provided as arguments to
    the function. This runner is used to change those options in isolation by loading a copy of the `tabulate` module.
    This helps to ensure that other calls to `tabulate.tabulate` don't see unexpected results.
    """

    _TABULATE_MODULE_COPY: Optional[ModuleType] = None
    _STATE_LOCK = threading.Lock()

    @classmethod
    def tabulate(
        cls,
        tabular_data: Sequence[Union[Mapping[str, CellValue], Sequence[CellValue]]],
        headers: Union[str, Sequence[str]],
        column_alignment: Optional[Sequence[str]] = None,
        tablefmt: str = "simple",
    ) -> str:
        """Produce a text table from the given data. Also see class docstring."""
        with _IsolatedTabulateRunner._STATE_LOCK:
            try:
                if _IsolatedTabulateRunner._TABULATE_MODULE_COPY is None:
                    tabulate_module_spec = importlib.util.find_spec("tabulate")
                    if tabulate_module_spec is None:
                        raise RuntimeError("Unable to find spec for `tabulate`.")
                    tabulate_module_copy = importlib.util.module_from_spec(tabulate_module_spec)
                    if tabulate_module_copy is None:
                        raise RuntimeError(f"Unable to load module using {tabulate_module_spec=}")
                    if tabulate_module_spec.loader is None:
                        raise RuntimeError(f"Loader missing for {tabulate_module_spec=}")
                    tabulate_module_spec.loader.exec_module(tabulate_module_copy)
                    tabulate_module_copy.PRESERVE_WHITESPACE = True  # type: ignore[attr-defined]
                    _IsolatedTabulateRunner._TABULATE_MODULE_COPY = tabulate_module_copy
            except Exception:
                logger.exception(
                    "Failed to load a copy of the `tabulate` module. This means that some table-formatting options "
                    "can't be applied. This is a bug and should be investigated."
                )

        # `tabulate.tabulate` can detect numeric values and apply special formatting rules. This can
        # result in unexpected values when coupled with the `--decimals` option, so disabling that feature.
        disable_numparse = True

        if _IsolatedTabulateRunner._TABULATE_MODULE_COPY is None:
            logger.warning(
                "Generating text table without required options set as there was an error loading the "
                "`tabulate` module."
            )
            return tabulate.tabulate(
                tabular_data=tabular_data,
                headers=headers,
                disable_numparse=disable_numparse,
                colalign=column_alignment,
                tablefmt=tablefmt,
            )

        return _IsolatedTabulateRunner._TABULATE_MODULE_COPY.tabulate(
            tabular_data=tabular_data,
            headers=headers,
            disable_numparse=disable_numparse,
            colalign=column_alignment,
            tablefmt=tablefmt,
        )
