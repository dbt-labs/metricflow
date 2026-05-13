test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: ClickHouse
---
SELECT
  SUM(__listings) AS listings
FROM (
  SELECT
    listings AS __listings
  FROM (
    SELECT
      subq_35.user__revenue_all_time AS user__revenue_all_time
      , subq_27.__listings AS listings
    FROM (
      SELECT
        user_id AS user
        , 1 AS __listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_27
    LEFT OUTER JOIN (
      SELECT
        user_id AS user
        , SUM(revenue) AS user__revenue_all_time
      FROM ***************************.fct_revenue revenue_src_28000
      GROUP BY
        user_id
    ) subq_35
    ON
      subq_27.user = subq_35.user
  ) subq_37
  WHERE user__revenue_all_time > 1
) subq_39
