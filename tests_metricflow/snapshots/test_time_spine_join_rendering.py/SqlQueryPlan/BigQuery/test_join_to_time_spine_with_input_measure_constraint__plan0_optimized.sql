-- Constrain Output with WHERE
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , booking__is_instant
  , bookings AS instant_bookings_with_measure_filter
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_17.ds AS metric_time__day
    , subq_15.booking__is_instant AS booking__is_instant
    , subq_15.bookings AS bookings
  FROM ***************************.mf_time_spine subq_17
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , booking__is_instant
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day', 'listing']
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , listing_id AS listing
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    WHERE ((booking__is_instant) AND (listing IS NOT NULL)) AND (metric_time__day > '2020-01-01')
    GROUP BY
      metric_time__day
      , booking__is_instant
  ) subq_15
  ON
    subq_17.ds = subq_15.metric_time__day
) subq_18
WHERE (booking__is_instant) AND (metric_time__day > '2020-01-01')
