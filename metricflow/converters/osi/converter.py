"""Re-export shim for backwards compatibility.

Import from the focused modules directly for new code:
  - metricflow.converters.osi.msi_to_osi  → MSIToOSIConverter
  - metricflow.converters.osi.osi_to_msi  → OSIToMSIConverter
  - metricflow.converters.osi.filter_utils → _render_filter_template
  - metricflow.converters.osi.expression_utils → _extract_agg_info, etc.
"""
from __future__ import annotations

from metricflow.converters.osi.filter_utils import _render_filter_template as _render_filter_template
from metricflow.converters.osi.msi_to_osi import MSIToOSIConverter as MSIToOSIConverter
from metricflow.converters.osi.osi_to_msi import OSIToMSIConverter as OSIToMSIConverter
