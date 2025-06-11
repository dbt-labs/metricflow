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

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import MutableGraphPath, NodeT, EdgeT
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from typing_extensions import Self, override

logger = logging.getLogger(__name__)

class TraversalStopEventType(OrderedEnum):
    VISIT_TARGET_NODE = "visit_target_node"
    VISIT_FINISHED_NODE = "visit_finished_node"

@fast_frozen_dataclass()
class TraversalStopEvent(Generic[NodeT, EdgeT]):
    event_type: TraversalStopEventType
    current_path: MutableGraphPath[NodeT, EdgeT]