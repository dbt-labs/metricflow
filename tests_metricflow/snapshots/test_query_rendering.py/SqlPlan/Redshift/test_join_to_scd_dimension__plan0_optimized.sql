test_name: test_join_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using a dimension with a validity window inside a measure constraint.
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_src_26000.capacity AS listing__capacity
    , nr_subq_7.metric_time__day AS metric_time__day
    , nr_subq_7.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
  ) nr_subq_7
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_26000
  ON
    (
      nr_subq_7.listing = listings_src_26000.listing_id
    ) AND (
      (
        nr_subq_7.metric_time__day >= listings_src_26000.active_from
      ) AND (
        (
          nr_subq_7.metric_time__day < listings_src_26000.active_to
        ) OR (
          listings_src_26000.active_to IS NULL
        )
      )
    )
) nr_subq_10
WHERE listing__capacity > 2
GROUP BY
  metric_time__day
