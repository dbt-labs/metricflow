test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    subq_26.user__revenue_all_time AS user__revenue_all_time
    , subq_19.__listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_19
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'revenue'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__revenue', 'user']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['user', 'user__revenue_all_time']
    SELECT
      user_id AS user
      , SUM(revenue) AS user__revenue_all_time
    FROM ***************************.fct_revenue revenue_src_28000
    GROUP BY
      user_id
  ) subq_26
  ON
    subq_19.user = subq_26.user
) subq_27
WHERE user__revenue_all_time > 1
