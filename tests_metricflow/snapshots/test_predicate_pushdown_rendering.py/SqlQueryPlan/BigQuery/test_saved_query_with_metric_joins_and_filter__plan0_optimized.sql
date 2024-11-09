test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_51.listing__capacity_latest, subq_61.listing__capacity_latest, subq_82.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_51.bookings) AS bookings
  , MAX(subq_61.views) AS views
  , MAX(CAST(subq_82.bookings AS FLOAT64) / CAST(NULLIF(subq_82.views, 0) AS FLOAT64)) AS bookings_per_view
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
    SELECT
      listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_43.metric_time__day AS metric_time__day
      , subq_43.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_43
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_43.listing = listings_latest_src_28000.listing_id
  ) subq_47
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) subq_51
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
    SELECT
      listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_53.metric_time__day AS metric_time__day
      , subq_53.views AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_53
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_53.listing = listings_latest_src_28000.listing_id
  ) subq_57
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) subq_61
ON
  subq_51.listing__capacity_latest = subq_61.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_71.listing__capacity_latest, subq_81.listing__capacity_latest) AS listing__capacity_latest
    , MAX(subq_71.bookings) AS bookings
    , MAX(subq_81.views) AS views
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
      SELECT
        listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , listings_latest_src_28000.capacity AS listing__capacity_latest
        , subq_63.metric_time__day AS metric_time__day
        , subq_63.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_63
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_63.listing = listings_latest_src_28000.listing_id
    ) subq_67
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_71
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
      SELECT
        listings_latest_src_28000.is_lux AS listing__is_lux_latest
        , listings_latest_src_28000.capacity AS listing__capacity_latest
        , subq_73.metric_time__day AS metric_time__day
        , subq_73.views AS views
      FROM (
        -- Read Elements From Semantic Model 'views_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , listing_id AS listing
          , 1 AS views
        FROM ***************************.fct_views views_source_src_28000
      ) subq_73
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_73.listing = listings_latest_src_28000.listing_id
    ) subq_77
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_81
  ON
    subq_71.listing__capacity_latest = subq_81.listing__capacity_latest
  GROUP BY
    listing__capacity_latest
) subq_82
ON
  COALESCE(subq_51.listing__capacity_latest, subq_61.listing__capacity_latest) = subq_82.listing__capacity_latest
GROUP BY
  listing__capacity_latest
