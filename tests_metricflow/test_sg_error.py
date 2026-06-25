from __future__ import annotations

import logging

from metricflow_semantics.test_helpers.manifest_helpers import (
    mf_load_manifest_from_yaml_directory,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.sg_08_ambiguous_multi_hop_join import (
    SG_08_AMBIGUOUS_MULTI_HOP_JOIN,
)

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.performance.test_profiling_examples import mf_explain_saved_query

logger = logging.getLogger(__name__)


def test_compare_ambiguous_join_path(sql_client: SqlClient) -> None:
    """Compare `explain` output between the legacy resolver and the SG resolver for an ambiguous join path."""
    manifest = mf_load_manifest_from_yaml_directory(
        SG_08_AMBIGUOUS_MULTI_HOP_JOIN.directory, template_mapping={"source_schema": "dummy_schema"}
    )
    explain_result = mf_explain_saved_query(
        manifest, sql_client, saved_query_names=["bookings_saved_query"], use_semantic_graph=False
    )
    assert explain_result is not None

    explain_result = mf_explain_saved_query(
        manifest, sql_client, saved_query_names=["bookings_saved_query"], use_semantic_graph=True
    )
    assert explain_result is None
