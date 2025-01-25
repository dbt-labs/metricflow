test_name: test_offset_window_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_16.booking__ds__day, nr_subq_24.booking__ds__day) AS booking__ds__day
    , MAX(nr_subq_16.bookings) AS bookings
    , MAX(nr_subq_24.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__ds__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'booking__ds__day']
      SELECT
        DATETIME_TRUNC(ds, day) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_14
    GROUP BY
      booking__ds__day
  ) nr_subq_16
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS booking__ds__day
      , SUM(nr_subq_17.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_17
    ON
      DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 14 day) = nr_subq_17.booking__ds__day
    GROUP BY
      booking__ds__day
  ) nr_subq_24
  ON
    nr_subq_16.booking__ds__day = nr_subq_24.booking__ds__day
  GROUP BY
    booking__ds__day
) nr_subq_25
