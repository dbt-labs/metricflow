from __future__ import annotations

import logging
import pathlib
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Generic, Optional, TypeVar

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from typing_extensions import Self, override

logger = logging.getLogger(__name__)


@singleton_dataclass()
class MetricDefinitionLabel(MetricflowGraphLabel):
    pass