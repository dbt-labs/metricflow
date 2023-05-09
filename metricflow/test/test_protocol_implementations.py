from dbt_semantic_interfaces.objects.elements.dimension import Dimension, DimensionValidityParams, DimensionTypeParams
from dbt_semantic_interfaces.objects.metadata import Metadata, FileSlice
from dbt_semantic_interfaces.protocols.dimension import Dimension as DimensionProtocol
from dbt_semantic_interfaces.protocols.metadata import Metadata as MetadataProtocol
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


def test_dimension_protocol():  # noqa: D
    dimension = Dimension(
        name="test_dim",
        type=DimensionType.CATEGORICAL,
        type_params=DimensionTypeParams(
            time_granularity=TimeGranularity.DAY,
            validity_params=DimensionValidityParams(),
        ),
    )
    assert isinstance(dimension, DimensionProtocol)


def test_metadata_protocol():  # noqa: D
    metadata = Metadata(
        repo_file_path="/path/to/cats.txt",
        file_slice=FileSlice(
            filename="cats.txt",
            content="I like cats",
            start_line_number=0,
            end_line_number=1,
        ),
    )
    assert isinstance(metadata, MetadataProtocol)
