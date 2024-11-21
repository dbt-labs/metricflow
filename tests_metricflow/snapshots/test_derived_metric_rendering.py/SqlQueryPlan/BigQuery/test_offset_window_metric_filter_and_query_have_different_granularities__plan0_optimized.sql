test_name: test_offset_window_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with one granularity and filtered by a different one.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__month, subq_30.metric_time__month) AS metric_time__month
    , MAX(subq_24.booking_value) AS booking_value
    , MAX(subq_30.bookers) AS bookers
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , SUM(booking_value) AS booking_value
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        DATETIME_TRUNC(subq_19.ds, month) AS metric_time__month
        , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS metric_time__day
        , bookings_source_src_28000.booking_value AS booking_value
      FROM ***************************.mf_time_spine subq_19
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        DATE_SUB(CAST(subq_19.ds AS DATETIME), INTERVAL 1 week) = DATETIME_TRUNC(bookings_source_src_28000.ds, day)
    ) subq_20
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_24
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookers', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , COUNT(DISTINCT bookers) AS bookers
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , DATETIME_TRUNC(ds, month) AS metric_time__month
        , guest_id AS bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_26
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_30
  ON
    subq_24.metric_time__month = subq_30.metric_time__month
  GROUP BY
    metric_time__month
) subq_31
