from __future__ import annotations

import logging

from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.singleton_factory import SingletonFactory

logger = logging.getLogger(__name__)


class SemanticModelIdFactory(SingletonFactory):
    _model_name_to_model_id: dict[str, SemanticModelId] = {}

    @classmethod
    def get_model_id(cls, model_name: str) -> SemanticModelId:
        model_id = cls._model_name_to_model_id.get(model_name)

        if model_id is None:
            model_id = SemanticModelId(model_name=model_name)
            cls._model_name_to_model_id[model_name] = model_id

        return model_id
