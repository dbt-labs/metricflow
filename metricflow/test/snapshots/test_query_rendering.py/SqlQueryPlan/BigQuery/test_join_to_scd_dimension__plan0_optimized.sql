-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__capacity', 'metric_time__day']
  SELECT
    subq_12.metric_time__day AS metric_time__day
    , listings_src_10017.capacity AS listing__capacity
    , subq_12.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10015
  ) subq_12
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_10017
  ON
    (
      subq_12.listing = listings_src_10017.listing_id
    ) AND (
      (
        subq_12.metric_time__day >= listings_src_10017.active_from
      ) AND (
        (
          subq_12.metric_time__day < listings_src_10017.active_to
        ) OR (
          listings_src_10017.active_to IS NULL
        )
      )
    )
) subq_16
WHERE listing__capacity > 2
GROUP BY
  metric_time__day
