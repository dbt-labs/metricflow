"""Tests for end to end object metadata parsing from the simple model YAML files"""

import os
from typing import Sequence

import pytest

from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.parsing.yaml_loader import YamlConfigLoader


def test_semantic_model_metadata_parsing(simple_semantic_manifest: SemanticManifest) -> None:
    """Tests internal metadata object parsing from a file into the Semantic Model model object

    This only tests some basic file name parsing for each semantic model since they are not guaranteed
    to be collected in the same file in the simple model, and the output here has been transformed
    so the YAML contents might or might not match.
    """
    assert len(simple_semantic_manifest.semantic_models) > 0
    for semantic_model in simple_semantic_manifest.semantic_models:
        assert (
            semantic_model.metadata is not None
        ), f"Metadata should always be parsed out of the model, but None found for semantic model: {semantic_model}!"
        _assert_metadata_filename_is_valid(semantic_model.metadata)


def test_metric_metadata_parsing(simple_semantic_manifest: SemanticManifest) -> None:
    """Tests internal metadata object parsing from a file into the Metric model object

    This only tests some basic file name parsing for each metric since they are not guaranteed
    to be collected in the same file in the simple model, and the output here has been transformed
    so the YAML contents might or might not match.
    """
    assert len(simple_semantic_manifest.metrics) > 0
    for metric in simple_semantic_manifest.metrics:
        assert (
            metric.metadata is not None
        ), f"Metadata should always be parsed out of the model, but None found for metric: {metric}!"
        _assert_metadata_filename_is_valid(metric.metadata)


@pytest.mark.skip("TODO: Determine what to do with measure proxy metric metadata")
def test_metric_metadata_parsing_with_measure_proxy(
    simple_semantic_manifest: SemanticManifest,
) -> None:
    """Tests internal metadata object parsing from a file into the Metric model object via measure proxy

    The simple model has a broader array of metric definitions, but it does not appear to have a measure proxy
    added via transformation. This test includes such a metric.

    This only tests some basic file name parsing for each metric since they are not guaranteed
    to be collected in the same file in the simple model, and the output here has been transformed
    so the YAML contents might or might not match.
    """
    assert len(simple_semantic_manifest.metrics) > 0
    for metric in simple_semantic_manifest.metrics:
        assert (
            metric.metadata is not None
        ), f"Metadata should always be parsed out of the model, but None found for metric: {metric}!"
        _assert_metadata_filename_is_valid(metric.metadata)


def test_measure_metadata_parsing(simple_semantic_manifest: SemanticManifest) -> None:
    """Tests internal metadata object parsing from a file into the Measure model object

    This tests the complete parsing process for Measure object metadata, including some baseline testing of things
    like file line number extraction and propagation. As with other cases, no assertions are made on the
    YAML contents themselves since they may change from the raw files into the SemanticManifest object we access
    here.
    """
    assert len(simple_semantic_manifest.semantic_models) > 0
    for semantic_model in simple_semantic_manifest.semantic_models:
        _assert_measure_metadata_is_valid(semantic_model.measures)


def _assert_metadata_filename_is_valid(metadata: Metadata) -> None:
    """Sequence of assertion steps to ensure the metadata object has consistent file name parsing"""
    assert YamlConfigLoader.is_valid_yaml_file_ending(metadata.repo_file_path), (
        f"Expected repo file path in measure metadata to be a yaml file with an appropriate ending. "
        f"Metadata: {metadata}"
    )

    assert (
        os.path.basename(metadata.repo_file_path) == metadata.file_slice.filename
    ), f"File name should be the final part of the repo file path. Metadata: {metadata}"
    assert (
        os.path.dirname(metadata.repo_file_path) != ""
    ), f"Expected repo file path to be fully resolved, but it is filename only. Metadata: {metadata}"


def _assert_measure_metadata_is_valid(measures: Sequence[Measure]) -> None:
    """Sequence of assertion steps to show that we are parsing metadata consistently for measures

    The assertions check that:
    1. Metadata is always set by the parser
    2. Metadata always contains a reasonable repo file path and file name
    3. Start and end line numbers are always increasing for every measure in the semantic model

    Since this test is operating on a transformed model, we do not make assertions about the raw YAML contents and how
    they relate to the properties of the measures themselves.
    """
    last_end_number = 0
    for measure in measures:
        assert (
            measure.metadata is not None
        ), f"Metadata should always be parsed out of the model, but None found for measure {measure}!"

        _assert_metadata_filename_is_valid(measure.metadata)

        assert measure.metadata.file_slice.start_line_number > last_end_number, (
            f"Start line numbers should always follow the end line number from the previous measure! "
            f"Metadata: {measure.metadata}"
        )
        assert measure.metadata.file_slice.end_line_number > measure.metadata.file_slice.start_line_number, (
            f"End line numbers should always be larger than start line numbers for a Measure definition entry! "
            f"Metadata: {measure.metadata}"
        )
        last_end_number = measure.metadata.file_slice.end_line_number
