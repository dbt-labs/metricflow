test_name: test_offset_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a derived offset metric.

      TODO: support metric time filters
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , listing__country_latest
    , SUM(bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      listings_latest_src_28000.country AS listing__country_latest
      , subq_25.metric_time__day AS metric_time__day
      , subq_25.booking__is_instant AS booking__is_instant
      , subq_25.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_25
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_25.listing = listings_latest_src_28000.listing_id
  ) subq_29
  WHERE booking__is_instant
  GROUP BY
    metric_time__day
    , listing__country_latest
)

, cm_7_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , listing__country_latest
    , SUM(bookings) AS bookings_2_weeks_ago
  FROM (
    -- Join Standard Outputs
    SELECT
      listings_latest_src_28000.country AS listing__country_latest
      , subq_38.metric_time__day AS metric_time__day
      , subq_38.booking__is_instant AS booking__is_instant
      , subq_38.bookings AS bookings
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_37.ds AS metric_time__day
        , subq_35.listing AS listing
        , subq_35.booking__is_instant AS booking__is_instant
        , subq_35.bookings AS bookings
      FROM ***************************.mf_time_spine subq_37
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_35
      ON
        DATEADD(day, -14, subq_37.ds) = subq_35.metric_time__day
    ) subq_38
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_38.listing = listings_latest_src_28000.listing_id
  ) subq_42
  WHERE booking__is_instant
  GROUP BY
    metric_time__day
    , listing__country_latest
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , listing__country_latest
    , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day) AS metric_time__day
      , COALESCE(cm_6_cte.listing__country_latest, cm_7_cte.listing__country_latest) AS listing__country_latest
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      (
        cm_6_cte.listing__country_latest = cm_7_cte.listing__country_latest
      ) AND (
        cm_6_cte.metric_time__day = cm_7_cte.metric_time__day
      )
    GROUP BY
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day)
      , COALESCE(cm_6_cte.listing__country_latest, cm_7_cte.listing__country_latest)
  ) subq_47
)

SELECT
  metric_time__day AS metric_time__day
  , listing__country_latest AS listing__country_latest
  , bookings_growth_2_weeks AS bookings_growth_2_weeks
FROM cm_8_cte cm_8_cte
