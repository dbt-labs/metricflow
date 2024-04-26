from __future__ import annotations

import logging
from typing import Mapping

import pytest
from dbt_semantic_interfaces.dataclass_serialization import DataClassDeserializer, DataclassSerializer
from metricflow_semantics.instances import InstanceSet

from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


@pytest.fixture
def serializer() -> DataclassSerializer:  # noqa: D103
    return DataclassSerializer()


@pytest.fixture
def deserializer() -> DataClassDeserializer:  # noqa: D103
    return DataClassDeserializer()


def test_serialization(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    serializer: DataclassSerializer,
    deserializer: DataClassDeserializer,
) -> None:
    for _, data_set in mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping.items():
        serialized_obj = serializer.pydantic_serialize(data_set.instance_set)
        deserialized_obj = deserializer.pydantic_deserialize(dataclass_type=InstanceSet, serialized_obj=serialized_obj)
        assert data_set.instance_set == deserialized_obj
