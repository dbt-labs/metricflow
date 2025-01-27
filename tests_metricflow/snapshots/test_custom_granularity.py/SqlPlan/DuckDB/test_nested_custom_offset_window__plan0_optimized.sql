test_name: test_nested_custom_offset_window
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
    , martian_day AS ds__martian_day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_offset_one_martian_day AS bookings_offset_one_martian_day_then_2_martian_days
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_52.ds__day__lead AS metric_time__day
    , subq_48.bookings_offset_one_martian_day AS bookings_offset_one_martian_day
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    WITH cte_15 AS (
      -- Read From CTE For node_id=rss_28018
      -- Get Custom Granularity Bounds
      SELECT
        ds__day
        , ds__martian_day
        , FIRST_VALUE(ds__day) OVER (
          PARTITION BY ds__martian_day
          ORDER BY ds__day
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day__first_value
        , LAST_VALUE(ds__day) OVER (
          PARTITION BY ds__martian_day
          ORDER BY ds__day
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day__last_value
        , ROW_NUMBER() OVER (
          PARTITION BY ds__martian_day
          ORDER BY ds__day
        ) AS ds__day__row_number
      FROM rss_28018_cte rss_28018_cte
    )

    SELECT
      cte_15.ds__day AS ds__day
      , CASE
        WHEN subq_51.ds__day__first_value__lead + INTERVAL (cte_15.ds__day__row_number - 1) day <= subq_51.ds__day__last_value__lead
          THEN subq_51.ds__day__first_value__lead + INTERVAL (cte_15.ds__day__row_number - 1) day
        ELSE NULL
      END AS ds__day__lead
    FROM cte_15 cte_15
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__martian_day
        , LEAD(ds__day__first_value, 2) OVER (ORDER BY ds__martian_day) AS ds__day__first_value__lead
        , LEAD(ds__day__last_value, 2) OVER (ORDER BY ds__martian_day) AS ds__day__last_value__lead
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        SELECT
          ds__martian_day
          , ds__day__first_value
          , ds__day__last_value
        FROM cte_15 cte_15
        GROUP BY
          ds__martian_day
          , ds__day__first_value
          , ds__day__last_value
      ) subq_50
    ) subq_51
    ON
      cte_15.ds__martian_day = subq_51.ds__martian_day
  ) subq_52
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , bookings AS bookings_offset_one_martian_day
    FROM (
      -- Join to Time Spine Dataset
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        subq_41.ds__day__lead AS metric_time__day
        , SUM(subq_37.bookings) AS bookings
      FROM (
        -- Offset Base Granularity By Custom Granularity Period(s)
        WITH cte_13 AS (
          -- Read From CTE For node_id=rss_28018
          -- Get Custom Granularity Bounds
          SELECT
            ds__day
            , ds__martian_day
            , FIRST_VALUE(ds__day) OVER (
              PARTITION BY ds__martian_day
              ORDER BY ds__day
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__day__first_value
            , LAST_VALUE(ds__day) OVER (
              PARTITION BY ds__martian_day
              ORDER BY ds__day
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__day__last_value
            , ROW_NUMBER() OVER (
              PARTITION BY ds__martian_day
              ORDER BY ds__day
            ) AS ds__day__row_number
          FROM rss_28018_cte rss_28018_cte
        )

        SELECT
          cte_13.ds__day AS ds__day
          , CASE
            WHEN subq_40.ds__day__first_value__lead + INTERVAL (cte_13.ds__day__row_number - 1) day <= subq_40.ds__day__last_value__lead
              THEN subq_40.ds__day__first_value__lead + INTERVAL (cte_13.ds__day__row_number - 1) day
            ELSE NULL
          END AS ds__day__lead
        FROM cte_13 cte_13
        INNER JOIN (
          -- Offset Custom Granularity Bounds
          SELECT
            ds__martian_day
            , LEAD(ds__day__first_value, 1) OVER (ORDER BY ds__martian_day) AS ds__day__first_value__lead
            , LEAD(ds__day__last_value, 1) OVER (ORDER BY ds__martian_day) AS ds__day__last_value__lead
          FROM (
            -- Get Unique Rows for Custom Granularity Bounds
            SELECT
              ds__martian_day
              , ds__day__first_value
              , ds__day__last_value
            FROM cte_13 cte_13
            GROUP BY
              ds__martian_day
              , ds__day__first_value
              , ds__day__last_value
          ) subq_39
        ) subq_40
        ON
          cte_13.ds__martian_day = subq_40.ds__martian_day
      ) subq_41
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_37
      ON
        subq_41.ds__day = subq_37.metric_time__day
      GROUP BY
        subq_41.ds__day__lead
    ) subq_47
  ) subq_48
  ON
    subq_52.ds__day = subq_48.metric_time__day
) subq_55
