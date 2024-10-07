-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_61.listing__capacity_latest, subq_73.listing__capacity_latest, subq_98.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_61.bookings) AS bookings
  , MAX(subq_73.views) AS views
  , MAX(CAST(subq_98.bookings AS DOUBLE PRECISION) / CAST(NULLIF(subq_98.views, 0) AS DOUBLE PRECISION)) AS bookings_per_view
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing__capacity_latest
    , SUM(bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
    SELECT
      subq_52.metric_time__day AS metric_time__day
      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_52.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_52
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_52.listing = listings_latest_src_28000.listing_id
  ) subq_57
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) subq_61
FULL OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['views', 'listing__capacity_latest']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing__capacity_latest
    , SUM(views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
    SELECT
      subq_64.metric_time__day AS metric_time__day
      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_64.views AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_64
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_64.listing = listings_latest_src_28000.listing_id
  ) subq_69
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) subq_73
ON
  subq_61.listing__capacity_latest = subq_73.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_85.listing__capacity_latest, subq_97.listing__capacity_latest) AS listing__capacity_latest
    , MAX(subq_85.bookings) AS bookings
    , MAX(subq_97.views) AS views
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      listing__capacity_latest
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['bookings', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_76.metric_time__day AS metric_time__day
        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , listings_latest_src_28000.capacity AS listing__capacity_latest
        , subq_76.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_76
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_76.listing = listings_latest_src_28000.listing_id
    ) subq_81
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_85
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['views', 'listing__capacity_latest']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      listing__capacity_latest
      , SUM(views) AS views
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['views', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_88.metric_time__day AS metric_time__day
        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , listings_latest_src_28000.capacity AS listing__capacity_latest
        , subq_88.views AS views
      FROM (
        -- Read Elements From Semantic Model 'views_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS views
        FROM ***************************.fct_views views_source_src_28000
      ) subq_88
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_88.listing = listings_latest_src_28000.listing_id
    ) subq_93
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_97
  ON
    subq_85.listing__capacity_latest = subq_97.listing__capacity_latest
  GROUP BY
    COALESCE(subq_85.listing__capacity_latest, subq_97.listing__capacity_latest)
) subq_98
ON
  COALESCE(subq_61.listing__capacity_latest, subq_73.listing__capacity_latest) = subq_98.listing__capacity_latest
GROUP BY
  COALESCE(subq_61.listing__capacity_latest, subq_73.listing__capacity_latest, subq_98.listing__capacity_latest)
