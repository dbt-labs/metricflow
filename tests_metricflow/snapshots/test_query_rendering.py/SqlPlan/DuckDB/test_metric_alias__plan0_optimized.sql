test_name: test_metric_alias
test_filename: test_query_rendering.py
docstring:
  Tests a plan with an aliased metric.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By ['bookings']
-- Change Column Aliases
SELECT
  metric_time__month
  , SUM(bookings) AS bookings_alias
FROM (
  -- Join Standard Outputs
  SELECT
    nr_subq_18.listing__bookings AS listing__bookings
    , nr_subq_15.metric_time__month AS metric_time__month
    , nr_subq_15.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_15
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_5
    GROUP BY
      listing
  ) nr_subq_18
  ON
    nr_subq_15.listing = nr_subq_18.listing
) nr_subq_19
WHERE listing__bookings > 2
GROUP BY
  metric_time__month
ORDER BY bookings_alias
