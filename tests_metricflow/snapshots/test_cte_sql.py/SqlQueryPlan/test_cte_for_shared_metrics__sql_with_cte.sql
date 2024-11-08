-- Combine Aggregated Outputs
WITH cm_0_cte AS (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_44.metric_time__day AS metric_time__day
    , listings_latest_src_28000.country AS listing__country_latest
    , SUM(subq_44.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_44
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_44.listing = listings_latest_src_28000.listing_id
  GROUP BY
    subq_44.metric_time__day
    , listings_latest_src_28000.country
)

, cm_1_cte AS (
  -- Join Standard Outputs
  -- Pass Only Elements: ['views', 'listing__country_latest', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_54.metric_time__day AS metric_time__day
    , listings_latest_src_28000.country AS listing__country_latest
    , SUM(subq_54.views) AS views
  FROM (
    -- Read Elements From Semantic Model 'views_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS views
    FROM ***************************.fct_views views_source_src_28000
  ) subq_54
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_54.listing = listings_latest_src_28000.listing_id
  GROUP BY
    subq_54.metric_time__day
    , listings_latest_src_28000.country
)

SELECT
  COALESCE(cm_0_cte.metric_time__day, cm_1_cte.metric_time__day, subq_64.metric_time__day) AS metric_time__day
  , COALESCE(cm_0_cte.listing__country_latest, cm_1_cte.listing__country_latest, subq_64.listing__country_latest) AS listing__country_latest
  , MAX(cm_0_cte.bookings) AS bookings
  , MAX(cm_1_cte.views) AS views
  , MAX(CAST(subq_64.bookings AS DOUBLE) / CAST(NULLIF(subq_64.views, 0) AS DOUBLE)) AS bookings_per_view
FROM cm_0_cte cm_0_cte
FULL OUTER JOIN
  cm_1_cte cm_1_cte
ON
  (
    cm_0_cte.listing__country_latest = cm_1_cte.listing__country_latest
  ) AND (
    cm_0_cte.metric_time__day = cm_1_cte.metric_time__day
  )
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(cm_0_cte.metric_time__day, cm_1_cte.metric_time__day) AS metric_time__day
    , COALESCE(cm_0_cte.listing__country_latest, cm_1_cte.listing__country_latest) AS listing__country_latest
    , MAX(cm_0_cte.bookings) AS bookings
    , MAX(cm_1_cte.views) AS views
  FROM cm_0_cte cm_0_cte
  FULL OUTER JOIN
    cm_1_cte cm_1_cte
  ON
    (
      cm_0_cte.listing__country_latest = cm_1_cte.listing__country_latest
    ) AND (
      cm_0_cte.metric_time__day = cm_1_cte.metric_time__day
    )
  GROUP BY
    COALESCE(cm_0_cte.metric_time__day, cm_1_cte.metric_time__day)
    , COALESCE(cm_0_cte.listing__country_latest, cm_1_cte.listing__country_latest)
) subq_64
ON
  (
    COALESCE(cm_0_cte.listing__country_latest, cm_1_cte.listing__country_latest) = subq_64.listing__country_latest
  ) AND (
    COALESCE(cm_0_cte.metric_time__day, cm_1_cte.metric_time__day) = subq_64.metric_time__day
  )
GROUP BY
  COALESCE(cm_0_cte.metric_time__day, cm_1_cte.metric_time__day, subq_64.metric_time__day)
  , COALESCE(cm_0_cte.listing__country_latest, cm_1_cte.listing__country_latest, subq_64.listing__country_latest)
