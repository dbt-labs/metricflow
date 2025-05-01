test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    subq_23.user__revenue_all_time AS user__revenue_all_time
    , subq_17.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_17
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'revenue'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['txn_revenue', 'user']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['user', 'user__revenue_all_time']
    SELECT
      user_id AS user
      , SUM(revenue) AS user__revenue_all_time
    FROM ***************************.fct_revenue revenue_src_28000
    GROUP BY
      user_id
  ) subq_23
  ON
    subq_17.user = subq_23.user
) subq_24
WHERE user__revenue_all_time > 1
