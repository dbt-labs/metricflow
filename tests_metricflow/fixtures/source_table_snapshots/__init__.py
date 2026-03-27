from __future__ import annotations

import logging

from metricflow_semantics.toolkit.directory_anchor import DirectoryPathAnchor

logger = logging.getLogger(__name__)

SOURCE_TABLE_SNAPSHOTS_DIRECTORY = DirectoryPathAnchor().directory
