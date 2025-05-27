from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols import SemanticManifest

from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import _check_manifest

logger = logging.getLogger(__name__)


def test_graph(sg_00_minimal_manifest: SemanticManifest) -> None:
    _check_manifest(sg_00_minimal_manifest)
