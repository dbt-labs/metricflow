-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookings', 'listing__capacity', 'metric_time__day']
  SELECT
    subq_7.metric_time__day AS metric_time__day
    , subq_7.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
  ) subq_7
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_26000
  ON
    (
      subq_7.listing = listings_src_26000.listing_id
    ) AND (
      (
        subq_7.metric_time__day >= listings_src_26000.active_from
      ) AND (
        (
          subq_7.metric_time__day < listings_src_26000.active_to
        ) OR (
          listings_src_26000.active_to IS NULL
        )
      )
    )
) subq_9
WHERE listing__capacity > 2
GROUP BY
  metric_time__day
