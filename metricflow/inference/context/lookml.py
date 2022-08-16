from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import glob
import os
import re
from typing import Iterable, List, Dict, Literal, Type, Union
from metricflow.inference.context.base import InferenceContext, InferenceContextProvider

import lkml

YesNo = Union[Literal["yes"], Literal["no"]]


def _bool_from_yesno(yesno: YesNo) -> bool:
    return yesno == "yes"


class LookMLMeasureType(str, Enum):
    MAX = "max"
    MEAN = "mean"
    MEDIAN = "median"
    SUM = "sum"
    SUM_DISTINCT = "sum_distinct"
    AVERAGE = "average"
    AVERAGE_DISTINCT = "average_distinct"
    COUNT = "count"
    COUNT_dISTINCT = "count_distinct"

    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> LookMLMeasureType:
        return LookMLMeasureType.UNKNOWN


@dataclass(frozen=True)
class LookMLMeasure(InferenceContext):
    """Encapsulates a LookML measure."""

    display_name: str
    sql: str
    type: LookMLMeasureType

    @staticmethod
    def from_dict(data: Dict) -> LookMLMeasure:
        return LookMLMeasure(
            sql=data.get("sql"), display_name=data.get("label", data["name"]), type=LookMLMeasureType(data.get("type"))
        )


class LookMLDimensionType(str, Enum):
    DATE = "date"
    DATETIME = "date_time"
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "yesno"

    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> LookMLDimensionType:
        return LookMLDimensionType.UNKNOWN


@dataclass(frozen=True)
class LookMLDimension(InferenceContext):
    """Encapsulates a LookML dimension."""

    display_name: str
    sql: str
    primary_key: bool
    type: LookMLDimensionType

    @staticmethod
    def from_dict(data: Dict) -> LookMLDimension:
        return LookMLDimension(
            sql=data.get("sql"),
            display_name=data.get("label", data["name"]),
            primary_key=_bool_from_yesno(data.get("primary_key", False)),
            type=LookMLDimensionType(data.get("type")),
        )


@dataclass(frozen=True)
class LookMLView(InferenceContext):
    """Encapsulates a LookML view. In practice, each view references a table."""

    table_name: str
    display_name: str

    dimensions: List[int]

    @staticmethod
    def from_dict(data: Dict) -> LookMLView:
        return LookMLView(
            table_name=data["name"],
            display_name=data.get("label", data["name"]),
            dimensions=[LookMLDimension.from_dict(dimension_dict) for dimension_dict in data.get("dimensions", [])],
        )


@dataclass(frozen=True)
class LookMLInferenceContext(InferenceContext):
    """Inference context from a LookML project."""

    views: List[LookMLView]


class LookMLInferenceContextProvider(InferenceContextProvider[LookMLInferenceContext]):
    """Use a LookML project to provide a `LookMLInferenceContext`."""

    provided_type: Type[LookMLInferenceContext] = LookMLInferenceContext

    def __init__(self, project_dir: str) -> None:
        """Initialize the context provider.

        project_dir: the LookML project directory.
        """

        project_dir = os.path.abspath(project_dir)

        if not os.path.isdir(project_dir):
            raise ValueError("`project_dir` must be an existing directory")

        self.project_dir = project_dir

    def _find_view_dicts_in_file_dict(self, file_dict: Dict) -> Iterable[Dict]:
        return file_dict.get("views", [])

    def get_context(self) -> LookMLInferenceContext:
        """Get the context for the LookML project."""

        glob_pattern = self.project_dir.rstrip(os.path.sep) + "/**/*.view.lkml"
        lookml_files = glob.glob(glob_pattern, recursive=True)

        views: List[LookMLView] = []
        for file_name in lookml_files:
            with open(file_name, "r") as file:
                file_dict = lkml.load(file)

            view_dicts = self._find_view_dicts_in_file_dict(file_dict)
            for view_dict in view_dicts:
                parsed_view = LookMLView.from_dict(view_dict)
                views.append(parsed_view)

        return LookMLInferenceContext(views=views)
