test_name: test_custom_offset_window_with_matching_grain_where_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__martian_day
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'booking__ds__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    booking__ds__martian_day
    , SUM(bookings) AS bookings
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_15.booking__ds__martian_day AS booking__ds__martian_day
      , subq_13.bookings AS bookings
    FROM (
      -- Join Offset Custom Granularity to Base Granularity
      WITH cte_6 AS (
        -- Read From Time Spine 'mf_time_spine'
        SELECT
          ds AS ds__day
          , martian_day AS ds__martian_day
        FROM ***************************.mf_time_spine time_spine_src_28006
      )

      SELECT
        cte_6.ds__day AS ds__day
        , subq_14.ds__martian_day__lead AS booking__ds__martian_day
      FROM cte_6 cte_6
      INNER JOIN (
        -- Offset Custom Granularity
        SELECT
          ds__martian_day
          , LEAD(ds__martian_day, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__lead
        FROM cte_6 cte_6
        GROUP BY
          ds__martian_day
      ) subq_14
      ON
        cte_6.ds__martian_day = subq_14.ds__martian_day
    ) subq_15
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS booking__ds__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_13
    ON
      subq_15.ds__day = subq_13.booking__ds__day
  ) subq_17
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    booking__ds__martian_day
) subq_21
