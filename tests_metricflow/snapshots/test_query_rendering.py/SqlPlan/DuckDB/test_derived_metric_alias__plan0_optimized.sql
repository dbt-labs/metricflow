test_name: test_derived_metric_alias
test_filename: test_query_rendering.py
docstring:
  Tests a plan with an aliased metric.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Order By ['booking_fees']
-- Change Column Aliases
SELECT
  metric_time__day
  , booking_value * 0.05 AS bookings_alias
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(booking_value) AS booking_value
  FROM (
    -- Join Standard Outputs
    SELECT
      nr_subq_22.listing__booking_fees AS listing__booking_fees
      , DATE_TRUNC('day', bookings_source_src_28000.ds) AS metric_time__day
      , bookings_source_src_28000.booking_value AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN (
      -- Compute Metrics via Expressions
      -- Pass Only Elements: ['listing', 'listing__booking_fees']
      SELECT
        listing
        , booking_value * 0.05 AS listing__booking_fees
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['booking_value', 'listing']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          listing_id AS listing
          , SUM(booking_value) AS booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
        GROUP BY
          listing_id
      ) nr_subq_20
    ) nr_subq_22
    ON
      bookings_source_src_28000.listing_id = nr_subq_22.listing
  ) nr_subq_23
  WHERE listing__booking_fees > 2
  GROUP BY
    metric_time__day
) nr_subq_27
ORDER BY bookings_alias
