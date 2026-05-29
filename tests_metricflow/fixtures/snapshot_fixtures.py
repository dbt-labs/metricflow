# from __future__ import annotations
#
# import logging
# from abc import ABC, abstractmethod
# from collections.abc import Mapping, Sequence, Set, Iterable
# from dataclasses import dataclass
# from pathlib import Path
# from typing import Protocol, TypeVar
#
# import pytest
# from _pytest.fixtures import FixtureRequest
#
# from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
# from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration, assert_str_snapshot_equal
# from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
# from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
# from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
# from metricflow_semantics.toolkit.string_helpers import mf_dedent
#
# logger = logging.getLogger(__name__)
#
# class SnapshotFixture:
#
#     def __init__(self, request: FixtureRequest, snapshot_configuration: SnapshotConfiguration) -> None:
#         self._request = request
#         self._snapshot_configuration = snapshot_configuration
#
#     def assert_me_snapshot_equal(self) -> None:
#         pass
#
#
# @pytest.fixture
# def snapshot_fixture(request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration) -> SnapshotFixture:
#     return
