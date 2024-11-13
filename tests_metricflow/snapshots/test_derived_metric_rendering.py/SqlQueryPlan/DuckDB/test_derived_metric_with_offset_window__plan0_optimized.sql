
    SELECT
      subq_22.ds AS metric_time__day
      , SUM(subq_20.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine subq_22
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_20
    ON
      subq_22.ds - INTERVAL 14 day = subq_20.metric_time__day
    GROUP BY
      subq_22.ds