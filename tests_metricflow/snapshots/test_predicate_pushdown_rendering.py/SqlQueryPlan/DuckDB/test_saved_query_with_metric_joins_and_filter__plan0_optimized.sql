-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_41.listing__capacity_latest, subq_49.listing__capacity_latest, subq_66.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_41.bookings) AS bookings
  , MAX(subq_49.views) AS views
  , MAX(CAST(subq_66.bookings AS DOUBLE) / CAST(NULLIF(subq_66.views, 0) AS DOUBLE)) AS bookings_per_view
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
      subq_35.metric_time__day AS metric_time__day
      , subq_35.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_35
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_35.listing = listings_latest_src_28000.listing_id
  ) subq_38
  WHERE ( listing__is_lux_latest ) AND ( metric_time__day >= '2020-01-02' )
  GROUP BY
    listing__capacity_latest
) subq_41
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
      subq_43.metric_time__day AS metric_time__day
      , subq_43.views AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_43
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_43.listing = listings_latest_src_28000.listing_id
  ) subq_46
  WHERE ( listing__is_lux_latest ) AND ( metric_time__day >= '2020-01-02' )
  GROUP BY
    listing__capacity_latest
) subq_49
ON
  subq_41.listing__capacity_latest = subq_49.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_57.listing__capacity_latest, subq_65.listing__capacity_latest) AS listing__capacity_latest
    , MAX(subq_57.bookings) AS bookings
    , MAX(subq_65.views) AS views
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
        subq_51.metric_time__day AS metric_time__day
        , subq_51.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_51
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_51.listing = listings_latest_src_28000.listing_id
    ) subq_54
    WHERE ( listing__is_lux_latest ) AND ( metric_time__day >= '2020-01-02' )
    GROUP BY
      listing__capacity_latest
  ) subq_57
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
        subq_59.metric_time__day AS metric_time__day
        , subq_59.views AS views
      FROM (
        -- Read Elements From Semantic Model 'views_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['views', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS views
        FROM ***************************.fct_views views_source_src_28000
      ) subq_59
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_59.listing = listings_latest_src_28000.listing_id
    ) subq_62
    WHERE ( listing__is_lux_latest ) AND ( metric_time__day >= '2020-01-02' )
    GROUP BY
      listing__capacity_latest
  ) subq_65
  ON
    subq_57.listing__capacity_latest = subq_65.listing__capacity_latest
  GROUP BY
    COALESCE(subq_57.listing__capacity_latest, subq_65.listing__capacity_latest)
) subq_66
ON
  COALESCE(subq_41.listing__capacity_latest, subq_49.listing__capacity_latest) = subq_66.listing__capacity_latest
GROUP BY
  COALESCE(subq_41.listing__capacity_latest, subq_49.listing__capacity_latest, subq_66.listing__capacity_latest)
