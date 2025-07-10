from __future__ import annotations

import difflib
import logging

from metricflow_semantics.helpers.table_helpers import IsolatedTabulateRunner
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.specs.spec_set import group_spec_by_type

from tests_metricflow_semantics.experimental.semantic_graph.table_helpers import RowColumnWidthEqualizer

logger = logging.getLogger(__name__)


def assert_linkable_element_sets_equal(  # noqa: D103
    left_set: BaseLinkableElementSet,
    right_set: BaseLinkableElementSet,
) -> None:
    headers = ("Dunder Name", "Metric-Subquery Entity-Links", "Type", "Properties", "Derived-From Semantic Models")

    left_rows = convert_linkable_element_set_to_rows(left_set)
    right_rows = convert_linkable_element_set_to_rows(right_set)

    equalizer = RowColumnWidthEqualizer()

    equalizer.add_headers(headers)
    equalizer.add_rows(left_rows)
    equalizer.add_rows(right_rows)

    new_left_rows = equalizer.reformat_rows(left_rows)
    new_right_rows = equalizer.reformat_rows(right_rows)

    left_table = IsolatedTabulateRunner.tabulate(tabular_data=new_left_rows, headers="keys", tablefmt="simple_outline")
    right_table = IsolatedTabulateRunner.tabulate(
        tabular_data=new_right_rows, headers="keys", tablefmt="simple_outline"
    )

    if left_table != right_table:
        diff_lines = difflib.unified_diff(
            a=left_table.splitlines(keepends=True),
            b=right_table.splitlines(keepends=True),
            fromfile="Left Result",
            tofile="Right Result",
        )
        diff = "".join(diff_lines)
        assert False, LazyFormat(
            "Mismatch between left and right sets", left_table=left_table, right_table=right_table, diff=diff
        )

    logger.debug(LazyFormat("Left and right sets match", spec_table=left_table))


def convert_linkable_element_set_to_rows(
    linkable_element_set: BaseLinkableElementSet,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    # ("Type", "Dunder Name", "Metric-Subquery Entity-Links", "Properties", "Derived-From Semantic Models")

    for annotated_spec in sorted(
        linkable_element_set.annotated_specs,
        key=lambda annotated_spec_in_lambda: annotated_spec_in_lambda.spec.qualified_name,
    ):
        # row: LinkableElementSetSnapshotRow = [annotated_spec.element_type.name, annotated_spec.spec.qualified_name]
        row_dict: dict[str, str] = {
            "Dunder Name": annotated_spec.spec.qualified_name.ljust(78),
        }
        spec_set = group_spec_by_type(annotated_spec.spec)

        if len(spec_set.group_by_metric_specs) == 0:
            # row.append(None)
            row_dict["Metric-Subquery Entity-Links"] = ""
        elif len(spec_set.group_by_metric_specs) == 1:
            group_by_metric_spec = spec_set.group_by_metric_specs[0]
            # row.append([entity_link.element_name for entity_link in group_by_metric_spec.metric_subquery_entity_links])
            row_dict["Metric-Subquery Entity-Links"] = ",".join(
                entity_link.element_name for entity_link in group_by_metric_spec.metric_subquery_entity_links
            )
        else:
            raise RuntimeError(LazyFormat("There should have been at most 1 group-by-metric spec", spec_set=spec_set))
        row_dict["Type"] = annotated_spec.element_type.name.ljust(14)
        # row.extend(
        #     (
        #         sorted(linkable_element_property.name for linkable_element_property in annotated_spec.properties),
        #         sorted(
        #             model_reference.semantic_model_name
        #             for model_reference in annotated_spec.derived_from_semantic_models
        #         ),
        #     )
        # )

        row_dict["Properties"] = ",".join(
            sorted(linkable_element_property.name for linkable_element_property in annotated_spec.properties)
        )
        row_dict["Derived-From Semantic Models"] = ",".join(
            sorted(
                model_reference.semantic_model_name for model_reference in annotated_spec.derived_from_semantic_models
            )
        )

        rows.append(row_dict)

    return rows
