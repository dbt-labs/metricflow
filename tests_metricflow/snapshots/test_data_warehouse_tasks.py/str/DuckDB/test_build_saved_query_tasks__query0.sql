SELECT
  COALESCE(subq_10.metric_time__day, subq_21.metric_time__day) AS metric_time__day
  , COALESCE(subq_10.listing__capacity_latest, subq_21.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_10.bookings) AS bookings
  , MAX(subq_21.instant_bookings) AS instant_bookings
FROM (
  SELECT
    metric_time__day
    , listing__capacity_latest
    , SUM(bookings) AS bookings
  FROM (
    SELECT
      subq_2.metric_time__day AS metric_time__day
      , listings_latest_src_10000.capacity AS listing__capacity_latest
      , subq_2.bookings AS bookings
    FROM (
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10000
    ) subq_2
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_10000
    ON
      subq_2.listing = listings_latest_src_10000.listing_id
  ) subq_7
  WHERE listing__capacity_latest > 3
  GROUP BY
    metric_time__day
    , listing__capacity_latest
) subq_10
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , listing__capacity_latest
    , SUM(instant_bookings) AS instant_bookings
  FROM (
    SELECT
      subq_13.metric_time__day AS metric_time__day
      , listings_latest_src_10000.capacity AS listing__capacity_latest
      , subq_13.instant_bookings AS instant_bookings
    FROM (
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      FROM ***************************.fct_bookings bookings_source_src_10000
    ) subq_13
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_10000
    ON
      subq_13.listing = listings_latest_src_10000.listing_id
  ) subq_18
  WHERE listing__capacity_latest > 3
  GROUP BY
    metric_time__day
    , listing__capacity_latest
) subq_21
ON
  (
    subq_10.listing__capacity_latest = subq_21.listing__capacity_latest
  ) AND (
    subq_10.metric_time__day = subq_21.metric_time__day
  )
GROUP BY
  COALESCE(subq_10.metric_time__day, subq_21.metric_time__day)
  , COALESCE(subq_10.listing__capacity_latest, subq_21.listing__capacity_latest)
