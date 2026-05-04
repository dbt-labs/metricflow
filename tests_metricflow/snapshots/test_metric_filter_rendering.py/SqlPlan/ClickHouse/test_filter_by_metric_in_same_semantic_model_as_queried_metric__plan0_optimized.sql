test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    guest_id AS guest
    , booking_value AS __booking_value
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COUNT(DISTINCT __bookers) AS bookers
FROM (
  SELECT
    bookers AS __bookers
  FROM (
    SELECT
      subq_29.guest__booking_value AS guest__booking_value
      , subq_23.__bookers AS bookers
    FROM (
      SELECT
        guest
        , __bookers
      FROM sma_28009_cte
    ) subq_23
    LEFT OUTER JOIN (
      SELECT
        guest
        , SUM(__booking_value) AS guest__booking_value
      FROM sma_28009_cte
      GROUP BY
        guest
    ) subq_29
    ON
      subq_23.guest = subq_29.guest
  ) subq_31
  WHERE guest__booking_value > 1.00
) subq_33
