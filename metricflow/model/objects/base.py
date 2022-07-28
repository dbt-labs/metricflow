from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar, Generic, Iterator, TypeVar

from pydantic import BaseModel, root_validator

from metricflow.errors.errors import ParsingException
from metricflow.model.parsing.yaml_loader import ParsingContext, PARSING_CONTEXT_KEY

# Type alias for the implicit "Any" type used as input and output for Pydantic's parsing API
PydanticParseableValueType = Any  # type: ignore[misc]


class HashableBaseModel(BaseModel):
    """Extends BaseModel with a generic hash function"""

    def __hash__(self) -> int:  # noqa: D
        return hash(json.dumps(self.json(sort_keys=True), sort_keys=True))


class FrozenBaseModel(HashableBaseModel):
    """Similar to HashableBaseModel but faux immutable."""

    class Config:
        """Pydantic feature."""

        allow_mutation = False

    def to_pretty_json(self) -> str:
        """Convert to a pretty JSON representation."""
        raw_json_str = self.json()
        json_obj = json.loads(raw_json_str)
        return json.dumps(json_obj, indent=4)

    def __str__(self) -> str:  # noqa: D
        return self.__repr__()


class ModelWithMetadataParsing(BaseModel):
    """Pydantic model object with a root validator for converting ParsingContext into Metadata]

    To use this validator the model class in question MUST have a field with the following base
    specification:

      metadata: Optional[Metadata]

    This class does NOT define the metadata itself because Pydantic's parsing and validation can be
    dependent on the order in which the model properties are listed in the class definition.
    Therefore, we prefer to keep this generic and allow callers to add the metadata field wherever
    we encounter parsing context information.
    """

    __METADATA_KEY__: ClassVar[str] = "metadata"

    @root_validator(pre=True)
    @classmethod
    def extract_metadata_from_parsing_context(cls, values: PydanticParseableValueType) -> PydanticParseableValueType:
        """Takes info from parsing context and converts it to a Metadata model object

        Per Pydantic's processing logic, this runs on the collection of input data for whatever model
        object is doing its parser walk. Since we set pre to True this should happen before any of the
        properties in the model is allowed to access the input values, which allows us to update them
        to include the appropriate inputs for defined Metadata object properties.
        """
        if not isinstance(values, dict):
            raise ValueError(
                f"Input values should be an object (dict) type, but got type({values}) with value: {values}"
            )

        # yes, this is confusing, but values is a dict, so it must have keys
        keys = values.keys()
        if cls.__METADATA_KEY__ in keys:
            # Sometimes people pass metadata in directly, e.g., in measure proxy metrics. Let Pydantic handle it.
            return values

        if PARSING_CONTEXT_KEY not in keys:
            # TODO: determine whether or not we want measure metadata tagged to measure proxy metrics. If we do,
            # add enforcement and make Metadata a non-optional element wherever it is set
            return values

        context = values.pop(PARSING_CONTEXT_KEY)
        if not isinstance(context, ParsingContext):
            raise ParsingException(
                f"Parsing context should always be a ParsingContext object, but we got a {type(context)} "
                f"with value: {context} inside payload: {values}"
            )

        values[cls.__METADATA_KEY__] = {
            "repo_file_path": context.filename,
            "file_slice": {
                "filename": os.path.split(context.filename)[-1],
                "content": context.content,
                "start_line_number": context.start_line,
                "end_line_number": context.end_line,
            },
        }
        return values


ModelObjectT_co = TypeVar("ModelObjectT_co", covariant=True, bound=BaseModel)


class PydanticCustomInputParser(ABC, Generic[ModelObjectT_co]):
    """Implements required methods for enabling custom parsing for Pydantic BaseModel objects

    This abstract class helper is for the specific case where model object classes need to do custom parsing
    prior to object initialization, meaning that the inputs to the initializer itself must be parsed in a
    manner custom to the object, but without updating the input values for the rest of the model hierarchy.

    This class will NOT allow for custom handling of dict or object type inputs, assuming that they should be
    processed and validated in the standard Pydantic way. Any subclass needing to mutate the inputs from a dict
    or object type in a subclass-specific way should still be able to do so via the standard Pydantic approach
    of adding a method decorated with @validator or @root_validator, which will work internally to initialization
    and validation of that model object itself.
    """

    @classmethod
    def __get_validators__(cls) -> Iterator[Callable]:
        """Pydantic magic method for allowing parsing of arbitrary input on parse_obj invocation

        This allow for parsing and validation prior to object initialization. Most classes implementing this
        interface in our model are doing so because the input value from user-supplied YAML will be a string
        representation rather than the structured object type.
        """
        yield cls.__parse_with_custom_handling

    @classmethod
    def __parse_with_custom_handling(cls: ModelObjectT_co, input: PydanticParseableValueType) -> ModelObjectT_co:
        """Core method for handling common valid - or easily validated - input types

        Pydantic objects can commonly appear as JSON object types (from, e.g., deserializing a Pydantic-serialized
        model) or direct instances of the model object class (from, e.g., initializing an object and passing it in
        to the initializer of a containing model object). This internal wrapper handles both of these cases in the
        standard Pydantic way of either validating the object type input on initialization, or passing the
        already-validated instance along as the output.

        Note this will also pass any Pydantic instance initialized via the `construct` method, which means it is
        possible for a caller to thread an unvalidated - and therefore improperly initialized - instance through.
        However, this is part of the contract with `construct` - it is only to be used when the inputs are known
        to the caller to be pre-validated, and so we do not bother guarding against that here.
        """
        if isinstance(input, dict):
            return cls(**input)
        elif isinstance(input, cls):
            return input
        else:
            return cls._from_yaml_value(input)

    @classmethod
    @abstractmethod
    def _from_yaml_value(cls: ModelObjectT_co, input: PydanticParseableValueType) -> ModelObjectT_co:
        """Abstract method for providing object-specific parsing logic"""
        raise NotImplementedError()
