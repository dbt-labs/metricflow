test_name: test_metric_filtered_by_itself
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query for a metric that filters by the same metric.
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    listing_id AS listing
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
      subq_29.listing__bookers AS listing__bookers
      , subq_23.__bookers AS bookers
    FROM (
      SELECT
        listing
        , __bookers
      FROM sma_28009_cte
    ) subq_23
    LEFT OUTER JOIN (
      SELECT
        listing
        , COUNT(DISTINCT __bookers) AS listing__bookers
      FROM sma_28009_cte
      GROUP BY
        listing
    ) subq_29
    ON
      subq_23.listing = subq_29.listing
  ) subq_31
  WHERE listing__bookers > 1.00
) subq_33
