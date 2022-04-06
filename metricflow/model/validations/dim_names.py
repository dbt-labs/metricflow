import logging
import pprint
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.identifier import Identifier, IdentifierType
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelObjectType
from metricflow.specs import (
    MeasureReference,
    DimensionReference,
    IdentifierReference,
    IdentifierSpec,
    DimensionSpec,
    LinklessIdentifierSpec,
)

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


@dataclass(frozen=True)
class IdentifierKey:
    """Identifier used for keys in dicts"""

    name: str
    type: IdentifierType


class DimensionAndIdentifierNameValidator:
    """Checks if a given dimension or identifier name is valid for querying with a given metric"""

    def __init__(self, model: UserConfiguredModel):  # noqa: D
        # Map the metric name to the measures that it uses
        self._metric_name_to_measures: Dict[str, List[MeasureReference]] = {}
        metric: Metric
        for metric in model.metrics:
            measure_references: List[MeasureReference] = []
            if metric.type_params:
                if metric.type_params.measure:
                    measure_references.append(metric.type_params.measure)
                if metric.type_params.measures:
                    measure_references.extend(metric.type_params.measures)
                if metric.type_params.numerator:
                    measure_references.append(metric.type_params.numerator)
                if metric.type_params.denominator:
                    measure_references.append(metric.type_params.denominator)
            self._metric_name_to_measures[metric.name] = measure_references

        # Map the identifier name / type to the associated dimensions
        # Keys are (name of identifier, IdentifierType value)
        # Values are the list of dimension names available.
        self._identifier_key_to_dimension_names: Dict[IdentifierKey, List[str]] = defaultdict(list)
        data_source: DataSource
        for data_source in model.data_sources:
            dimension_names = [x.name.element_name for x in data_source.dimensions]
            identifier: Identifier
            for identifier in data_source.identifiers:
                identifier_key = IdentifierKey(identifier.name.element_name, identifier.type)
                self._identifier_key_to_dimension_names[identifier_key].extend(dimension_names)

        # Map the measure to the identifier name / type present in the data source where the measure is defined.
        # Keys are the name of the measure
        # Values are (name of identifier, IdentifierType value)
        self._measure_to_associated_identifier_keys: Dict[MeasureReference, List[IdentifierKey]] = defaultdict(list)
        for data_source in model.data_sources:
            measure_references = [x.name for x in data_source.measures]
            # List of identifiers and their types in the data source
            identifier_keys: List[IdentifierKey] = []
            for identifier in data_source.identifiers:
                identifier_keys.append(IdentifierKey(identifier.name.element_name, identifier.type))
            for measure_reference in measure_references:
                self._measure_to_associated_identifier_keys[measure_reference].extend(identifier_keys)

        # Maps the type of identifier associated with the measure to the valid types of other identifiers associated
        # with dimensions
        self._measure_identifier_type_to_dimension_identifier_types = {
            IdentifierType.PRIMARY: [IdentifierType.PRIMARY, IdentifierType.UNIQUE],
            IdentifierType.UNIQUE: [IdentifierType.PRIMARY, IdentifierType.UNIQUE],
            IdentifierType.FOREIGN: [IdentifierType.PRIMARY, IdentifierType.UNIQUE],
        }

        # Think about case where measures are defined in two different data sources but with a different set of
        # dimensions or identifiers. Like tests/integration/semantics/itest_constraints.py::test_split_measure

        # Populate the list of local dimension for each measure
        self._measure_to_local_dimension: Dict[MeasureReference, List[DimensionReference]] = defaultdict(list)
        for data_source in model.data_sources:
            local_dimension_references = [dimension.name for dimension in data_source.dimensions]
            for measure in data_source.measures:
                self._measure_to_local_dimension[measure.name].extend(local_dimension_references)

        # Populate the list of identifiers for each measure
        self._measures_to_identifiers: Dict[MeasureReference, List[IdentifierReference]] = defaultdict(list)
        for data_source in model.data_sources:
            identifier_references = [identifier.name for identifier in data_source.identifiers]
            for measure in data_source.measures:
                self._measures_to_identifiers[measure.name].extend(identifier_references)

        # Populate the name of the element to the type
        self._element_name_to_type: Dict[str, ModelObjectType] = {}
        for data_source in model.data_sources:
            for measure in data_source.measures:
                self._element_name_to_type[measure.name.element_name] = ModelObjectType.MEASURE
            for dimension in data_source.dimensions:
                self._element_name_to_type[dimension.name.element_name] = ModelObjectType.DIMENSION
            for identifier in data_source.identifiers:
                self._element_name_to_type[identifier.name.element_name] = ModelObjectType.IDENTIFIER

    def _is_identifier_valid_for_measure(self, measure_reference: MeasureReference, identifier_name: str) -> bool:
        assert measure_reference in self._measures_to_identifiers
        identifier_spec = IdentifierSpec.parse(identifier_name)
        return identifier_spec in self._measures_to_identifiers[measure_reference]

    def is_dimension_valid_for_measure(self, measure_reference: MeasureReference, dimension_name: str) -> bool:
        """Return true iff the given measure and dimension could be queried together."""

        logger.debug(
            f"Checking if dimension name `{dimension_name} is valid for measure `{measure_reference.element_name}` "
        )
        dimension_spec = DimensionSpec.parse(dimension_name)

        # If there's no identifier, it must be a local dimension, so check there.
        if not dimension_spec.identifier_links:
            assert measure_reference in self._measure_to_local_dimension
            dimension_reference = DimensionReference(element_name=dimension_spec.element_name)
            return dimension_reference in self._measure_to_local_dimension[measure_reference]

        # It's a joined dimension since it has an identifier in the name.
        assert measure_reference in self._measure_to_associated_identifier_keys

        # The measure has these identifiers associated with them.
        for identifier_key in self._measure_to_associated_identifier_keys[measure_reference]:
            logger.debug(
                f"Measure {measure_reference.element_name} is associated with the identifier {identifier_key.name} of "
                f"type {identifier_key.type}"
            )
            # List of identifier types that may allow a joined dimension
            dimension_identifier_types = self._measure_identifier_type_to_dimension_identifier_types[
                identifier_key.type
            ]
            for dimension_identifier_type in dimension_identifier_types:
                logger.debug(
                    f"Looking for dimensions associated with identifiers with name {identifier_key.name} "
                    f"and type {identifier_key.type}"
                )

                if identifier_key in self._identifier_key_to_dimension_names:
                    dimension_element_names_with_given_identifier_name_and_type = (
                        self._identifier_key_to_dimension_names[
                            IdentifierKey(identifier_key.name, dimension_identifier_type)
                        ]
                    )
                    logger.debug(
                        f"Dimensions in data sources that contain identifiers named {identifier_key.name} "
                        f"and type {dimension_identifier_type} are "
                        f"{pp.pformat(dimension_element_names_with_given_identifier_name_and_type)}"
                    )

                    for dimension_element_name in dimension_element_names_with_given_identifier_name_and_type:
                        if (
                            dimension_spec.element_name == dimension_element_name
                            and LinklessIdentifierSpec.from_element_name(identifier_key.name)
                            in dimension_spec.identifier_links
                        ):
                            return True
                else:
                    logger.debug("Did not find associated dimensions.")
        logger.debug(f"Dimension name `{dimension_name}` is not valid for measure `{measure_reference.element_name}`")
        return False

    def is_dimension_valid_for_metric(self, metric_name: str, dimension_name: str) -> bool:
        """Return true iff the given dimension is valid to query with the given metric."""

        assert metric_name in self._metric_name_to_measures
        return all(
            self.is_dimension_valid_for_measure(measure_spec, dimension_name)
            for measure_spec in self._metric_name_to_measures[metric_name]
        )

    def is_identifier_valid_for_metric(self, metric_name: str, identifier_name: str) -> bool:
        """Return true iff the given identifier is valid to query with the given metric."""
        assert len(self._metric_name_to_measures[metric_name]) >= 1
        return all(
            self._is_identifier_valid_for_measure(measure_spec, identifier_name)
            for measure_spec in self._metric_name_to_measures[metric_name]
        )
