from __future__ import annotations

import logging
from typing import Optional

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest

from metricflow_semantics.model.semantics.linkable_spec_index import LinkableSpecIndex
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


class SemanticManifestLookup:
    """Adds semantics information to the user configured model."""

    def __init__(  # noqa: D107
        self, semantic_manifest: SemanticManifest, linkable_spec_index: Optional[LinkableSpecIndex] = None
    ) -> None:
        self._semantic_manifest = semantic_manifest
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
        self.custom_granularities = TimeSpineSource.build_custom_granularities(list(self._time_spine_sources.values()))
        self._semantic_model_lookup = SemanticModelLookup(
            model=semantic_manifest, custom_granularities=self.custom_granularities
        )
        if linkable_spec_index is None:
            self._metric_lookup = MetricLookup.create(
                semantic_manifest=self._semantic_manifest,
                semantic_model_lookup=self._semantic_model_lookup,
                custom_granularities=self.custom_granularities,
            )
        else:
            self._metric_lookup = MetricLookup.create_using_index(
                semantic_manifest=self._semantic_manifest,
                semantic_model_lookup=self._semantic_model_lookup,
                custom_granularities=self.custom_granularities,
                linkable_spec_index=linkable_spec_index,
            )

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D102
        return self._semantic_manifest

    @property
    def semantic_model_lookup(self) -> SemanticModelLookup:  # noqa: D102
        return self._semantic_model_lookup

    @property
    def metric_lookup(self) -> MetricLookup:  # noqa: D102
        return self._metric_lookup
