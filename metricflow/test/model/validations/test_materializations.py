import logging

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.materializations import ValidMaterializationRule
from metricflow.test.model.validations.helpers import materialization_with_guaranteed_meta
from metricflow.test.test_utils import (
    model_with_materialization,
)

logger = logging.getLogger(__name__)


def test_materialization_validation(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    ValidMaterializationRule.validate_model(simple_model__pre_transforms)


def test_identifier(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    assert (
        len(
            ValidMaterializationRule.validate_model(
                model_with_materialization(
                    simple_model__pre_transforms,
                    [
                        materialization_with_guaranteed_meta(
                            name="foobar",
                            metrics=["bookings"],
                            dimensions=["metric_time", "listing"],
                        )
                    ],
                )
            )
        )
        == 0
    )


def test_invalid_metric_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    assert (
        len(
            ValidMaterializationRule.validate_model(
                model_with_materialization(
                    simple_model__pre_transforms,
                    [
                        materialization_with_guaranteed_meta(
                            name="foobar",
                            metrics=["invalid_bookings"],
                            dimensions=["metric_time"],
                        )
                    ],
                )
            )
        )
        == 1
    )


def test_invalid_dimension_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    assert (
        len(
            ValidMaterializationRule.validate_model(
                model_with_materialization(
                    simple_model__pre_transforms,
                    [
                        materialization_with_guaranteed_meta(
                            name="foobar",
                            metrics=["bookings"],
                            dimensions=["metric_time", "invalid_dimension_name"],
                        )
                    ],
                )
            )
        )
        == 1
    )


def test_missing_primary_time_dimension(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    """Materializations should have the primary time dimension listed as a dimension"""
    assert (
        len(
            ValidMaterializationRule.validate_model(
                model_with_materialization(
                    simple_model__pre_transforms,
                    [
                        materialization_with_guaranteed_meta(
                            name="foobar",
                            metrics=["bookings"],
                            dimensions=["is_instant"],
                        )
                    ],
                )
            )
        )
        == 1
    )


def test_valid_time_granularity(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    assert (
        len(
            ValidMaterializationRule.validate_model(
                model_with_materialization(
                    simple_model__pre_transforms,
                    [
                        materialization_with_guaranteed_meta(
                            name="materialization_test_case",
                            metrics=["bookings"],
                            dimensions=["metric_time__day"],
                        )
                    ],
                )
            )
        )
        == 0
    )


def test_invalid_time_granularity(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    assert (
        len(
            ValidMaterializationRule.validate_model(
                model_with_materialization(
                    simple_model__pre_transforms,
                    [
                        materialization_with_guaranteed_meta(
                            name="materialization_test_case",
                            metrics=["revenue"],
                            dimensions=["metric_time__hour"],
                        )
                    ],
                )
            )
        )
        == 2
    )
