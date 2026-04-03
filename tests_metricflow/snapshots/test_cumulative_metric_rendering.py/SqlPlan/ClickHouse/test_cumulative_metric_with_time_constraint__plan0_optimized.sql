test_name: test_cumulative_metric_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric query with an adjustable time constraint.

      Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
      span of input data for a cumulative metric, but when we receive a time constraint filter expression we can
      automatically adjust it should render a query similar to this one.
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , SUM(__revenue) AS trailing_2_months_revenue
FROM (
  SELECT
    subq_23.metric_time__day AS metric_time__day
    , subq_22.__revenue AS __revenue
  FROM (
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_24
    WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
  ) subq_23
  INNER JOIN (
    SELECT
      toStartOfDay(created_at) AS metric_time__day
      , revenue AS __revenue
    FROM ***************************.fct_revenue revenue_src_28000
    WHERE toStartOfDay(created_at) BETWEEN '2019-11-01' AND '2020-01-01'
  ) subq_22
  ON
    (
      subq_22.metric_time__day <= subq_23.metric_time__day
    ) AND (
      subq_22.metric_time__day > addMonths(subq_23.metric_time__day, -2)
    )
  WHERE subq_23.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
) subq_28
GROUP BY
  metric_time__day
