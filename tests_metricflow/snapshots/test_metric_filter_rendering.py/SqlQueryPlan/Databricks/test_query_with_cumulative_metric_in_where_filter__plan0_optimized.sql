test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_4
WITH cm_3_cte AS (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['txn_revenue', 'user']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    user_id AS user
    , SUM(revenue) AS user__revenue_all_time
  FROM ***************************.fct_revenue revenue_src_28000
  GROUP BY
    user_id
)

, cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(listings) AS listings
  FROM (
    -- Join Standard Outputs
    SELECT
      cm_3_cte.user__revenue_all_time AS user__revenue_all_time
      , subq_13.listings AS listings
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      -- Metric Time Dimension 'ds'
      SELECT
        user_id AS user
        , 1 AS listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_13
    LEFT OUTER JOIN
      cm_3_cte cm_3_cte
    ON
      subq_13.user = cm_3_cte.user
  ) subq_20
  WHERE user__revenue_all_time > 1
)

SELECT
  listings AS listings
FROM cm_4_cte cm_4_cte
