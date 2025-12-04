test_name: test_scd_group_by_and_coarser_grain
test_filename: test_query_rendering.py
sql_engine: Snowflake
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__family_bookings', 'listing__capacity', 'metric_time__month']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__month
  , listing__capacity
  , SUM(family_bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__family_bookings', 'listing__capacity', 'metric_time__month']
  SELECT
    subq_11.metric_time__month AS metric_time__month
    , listings_src_26000.capacity AS listing__capacity
    , subq_11.__family_bookings AS family_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
      , listing_id AS listing
      , 1 AS __family_bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
  ) subq_11
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_26000
  ON
    (
      subq_11.listing = listings_src_26000.listing_id
    ) AND (
      (
        subq_11.metric_time__day >= listings_src_26000.active_from
      ) AND (
        (
          subq_11.metric_time__day < listings_src_26000.active_to
        ) OR (
          listings_src_26000.active_to IS NULL
        )
      )
    )
) subq_15
WHERE listing__capacity > 2
GROUP BY
  metric_time__month
  , listing__capacity
