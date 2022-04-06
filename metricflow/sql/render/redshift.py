from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer


class RedshiftSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Redshift engine."""

    @property
    def double_data_type(self) -> str:
        """Custom double data type for the Redshift engine"""
        return "DOUBLE PRECISION"


class RedshiftSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Redshift engine."""

    EXPR_RENDERER = RedshiftSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
