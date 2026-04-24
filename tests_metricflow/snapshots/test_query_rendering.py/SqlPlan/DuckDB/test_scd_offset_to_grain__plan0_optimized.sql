test_name: test_scd_offset_to_grain
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__month
  , listing__capacity
  , bookings_all_time
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__month
    , listing__capacity
    , LAST_VALUE(bookings_all_time) OVER (
      PARTITION BY
        listing__capacity
        , metric_time__month
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS bookings_all_time
  FROM (
    -- Join Standard Outputs
    -- Select: ['__bookings', 'listing__capacity', 'metric_time__month', 'metric_time__day']
    -- Select: ['__bookings', 'listing__capacity', 'metric_time__month', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Compute Metrics via Expressions
    SELECT
      subq_19.metric_time__day AS metric_time__day
      , subq_19.metric_time__month AS metric_time__month
      , listings_src_26000.capacity AS listing__capacity
      , SUM(subq_19.__bookings) AS bookings_all_time
    FROM (
      -- Join Self Over Time Range
      SELECT
        subq_18.ds AS metric_time__day
        , DATE_TRUNC('month', subq_18.ds) AS metric_time__month
        , subq_16.listing AS listing
        , subq_16.__bookings AS __bookings
      FROM ***************************.mf_time_spine subq_18
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_26000
      ) subq_16
      ON
        (subq_16.metric_time__day <= subq_18.ds)
    ) subq_19
    LEFT OUTER JOIN
      ***************************.dim_listings listings_src_26000
    ON
      (
        subq_19.listing = listings_src_26000.listing_id
      ) AND (
        (
          subq_19.metric_time__day >= listings_src_26000.active_from
        ) AND (
          (
            subq_19.metric_time__day < listings_src_26000.active_to
          ) OR (
            listings_src_26000.active_to IS NULL
          )
        )
      )
    GROUP BY
      subq_19.metric_time__day
      , subq_19.metric_time__month
      , listings_src_26000.capacity
  ) subq_27
) subq_28
GROUP BY
  metric_time__month
  , listing__capacity
  , bookings_all_time
