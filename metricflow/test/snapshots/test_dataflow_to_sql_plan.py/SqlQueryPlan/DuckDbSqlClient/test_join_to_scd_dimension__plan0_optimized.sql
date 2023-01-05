-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'metric_time']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__capacity', 'metric_time']
  SELECT
    subq_12.metric_time AS metric_time
    , listings_src_10020.capacity AS listing__capacity
    , subq_12.bookings AS bookings
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'metric_time', 'listing']
    SELECT
      ds AS metric_time
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10018
  ) subq_12
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_10020
  ON
    (
      subq_12.listing = listings_src_10020.listing_id
    ) AND (
      (
        subq_12.metric_time >= listings_src_10020.active_from
      ) AND (
        (
          subq_12.metric_time < listings_src_10020.active_to
        ) OR (
          listings_src_10020.active_to IS NULL
        )
      )
    )
) subq_16
WHERE listing__capacity > 2
GROUP BY
  metric_time
