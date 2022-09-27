from __future__ import annotations
from metricflow.sql.render.expr_renderer import DefaultSqlExpressionRenderer, SqlExpressionRenderer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer


class DatabricksSQLExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Databricks engine."""

    pass


class DatabricksSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Databricks engine."""

    EXPR_RENDERER = DatabricksSQLExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
