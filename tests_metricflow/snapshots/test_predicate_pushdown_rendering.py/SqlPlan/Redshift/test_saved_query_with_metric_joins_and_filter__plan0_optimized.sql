test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: Redshift
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(nr_subq_27.listing__capacity_latest, nr_subq_35.listing__capacity_latest, nr_subq_38.listing__capacity_latest) AS listing__capacity_latest
  , MAX(nr_subq_27.bookings) AS bookings
  , MAX(nr_subq_35.views) AS views
  , MAX(CAST(nr_subq_38.bookings AS DOUBLE PRECISION) / CAST(NULLIF(nr_subq_38.views, 0) AS DOUBLE PRECISION)) AS bookings_per_view
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
      , nr_subq_20.metric_time__day AS metric_time__day
      , nr_subq_20.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_20
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      nr_subq_20.listing = listings_latest_src_28000.listing_id
  ) nr_subq_23
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) nr_subq_27
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
      , nr_subq_28.metric_time__day AS metric_time__day
      , nr_subq_28.views AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) nr_subq_28
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      nr_subq_28.listing = listings_latest_src_28000.listing_id
  ) nr_subq_31
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) nr_subq_35
ON
  nr_subq_27.listing__capacity_latest = nr_subq_35.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_36.listing__capacity_latest, nr_subq_37.listing__capacity_latest) AS listing__capacity_latest
    , MAX(nr_subq_36.bookings) AS bookings
    , MAX(nr_subq_37.views) AS views
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
        , nr_subq_20.metric_time__day AS metric_time__day
        , nr_subq_20.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) nr_subq_20
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        nr_subq_20.listing = listings_latest_src_28000.listing_id
    ) nr_subq_23
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) nr_subq_36
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
        , nr_subq_28.metric_time__day AS metric_time__day
        , nr_subq_28.views AS views
      FROM (
        -- Read Elements From Semantic Model 'views_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS views
        FROM ***************************.fct_views views_source_src_28000
      ) nr_subq_28
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        nr_subq_28.listing = listings_latest_src_28000.listing_id
    ) nr_subq_31
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) nr_subq_37
  ON
    nr_subq_36.listing__capacity_latest = nr_subq_37.listing__capacity_latest
  GROUP BY
    COALESCE(nr_subq_36.listing__capacity_latest, nr_subq_37.listing__capacity_latest)
) nr_subq_38
ON
  COALESCE(nr_subq_27.listing__capacity_latest, nr_subq_35.listing__capacity_latest) = nr_subq_38.listing__capacity_latest
GROUP BY
  COALESCE(nr_subq_27.listing__capacity_latest, nr_subq_35.listing__capacity_latest, nr_subq_38.listing__capacity_latest)
