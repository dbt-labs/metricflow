# from __future__ import annotations
#
# import logging
# import pathlib
# from abc import ABC, abstractmethod
# from collections import defaultdict
# from collections.abc import Mapping, Sequence, Set
# from dataclasses import dataclass
# from enum import Enum
# from pathlib import Path
# from typing import Callable, Generic, Optional, TypeVar
#
# import msgspec
# from dbt_semantic_interfaces.protocols import SemanticManifest
#
# from metricflow_semantics.experimental.semantic_graph.entity_id import EntityId
# from metricflow_semantics.helpers.string_helpers import mf_dedent
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
# from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
# from typing_extensions import Self, override
#
# from metricflow_semantics.time.time_spine_source import TimeSpineSource
#
# logger = logging.getLogger(__name__)
#
#
# class SemanticGraphNodeLookup:
#
#     def get_time_entities(self, semantic_manifest: SemanticManifest) -> Sequence[EntityId]:
#         time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
#         custom_granularities = TimeSpineSource.build_custom_granularities(time_spine_sources.values())
#
#
