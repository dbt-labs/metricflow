test_name: test_offset_to_grain_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__day
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_16.booking__ds__day, nr_subq_24.booking__ds__day) AS booking__ds__day
    , MAX(nr_subq_16.bookings) AS bookings
    , MAX(nr_subq_24.bookings_at_start_of_month) AS bookings_at_start_of_month
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
        DATE_TRUNC('day', ds) AS booking__ds__day
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
      , SUM(nr_subq_17.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_17
    ON
      DATE_TRUNC('month', time_spine_src_28006.ds) = nr_subq_17.booking__ds__day
    GROUP BY
      time_spine_src_28006.ds
  ) nr_subq_24
  ON
    nr_subq_16.booking__ds__day = nr_subq_24.booking__ds__day
  GROUP BY
    COALESCE(nr_subq_16.booking__ds__day, nr_subq_24.booking__ds__day)
) nr_subq_25
