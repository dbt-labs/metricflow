test_name: test_build_saved_query_tasks
test_filename: test_data_warehouse_tasks.py
sql_engine: Postgres
---
WITH sma_10009_cte AS (
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS __bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
  FROM ***************************.fct_bookings bookings_source_src_10000
)

, sma_10014_cte AS (
  SELECT
    listing_id AS listing
    , capacity AS capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10000
)

SELECT
  COALESCE(subq_9.metric_time__day, subq_17.metric_time__day) AS metric_time__day
  , COALESCE(subq_9.listing__capacity_latest, subq_17.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_9.bookings) AS bookings
  , MAX(subq_17.instant_bookings) AS instant_bookings
FROM (
  SELECT
    metric_time__day
    , listing__capacity_latest
    , SUM(bookings) AS bookings
  FROM (
    SELECT
      sma_10014_cte.capacity_latest AS listing__capacity_latest
      , sma_10009_cte.metric_time__day AS metric_time__day
      , sma_10009_cte.__bookings AS bookings
    FROM sma_10009_cte
    LEFT OUTER JOIN
      sma_10014_cte
    ON
      sma_10009_cte.listing = sma_10014_cte.listing
  ) subq_5
  WHERE listing__capacity_latest > 3
  GROUP BY
    metric_time__day
    , listing__capacity_latest
) subq_9
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , listing__capacity_latest
    , SUM(instant_bookings) AS instant_bookings
  FROM (
    SELECT
      sma_10014_cte.capacity_latest AS listing__capacity_latest
      , sma_10009_cte.metric_time__day AS metric_time__day
      , sma_10009_cte.__instant_bookings AS instant_bookings
    FROM sma_10009_cte
    LEFT OUTER JOIN
      sma_10014_cte
    ON
      sma_10009_cte.listing = sma_10014_cte.listing
  ) subq_13
  WHERE listing__capacity_latest > 3
  GROUP BY
    metric_time__day
    , listing__capacity_latest
) subq_17
ON
  (
    subq_9.listing__capacity_latest = subq_17.listing__capacity_latest
  ) AND (
    subq_9.metric_time__day = subq_17.metric_time__day
  )
GROUP BY
  COALESCE(subq_9.metric_time__day, subq_17.metric_time__day)
  , COALESCE(subq_9.listing__capacity_latest, subq_17.listing__capacity_latest)
