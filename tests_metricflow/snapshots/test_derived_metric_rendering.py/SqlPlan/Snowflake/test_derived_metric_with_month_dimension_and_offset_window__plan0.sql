test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  subq_8.metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_7.metric_time__month
    , subq_7.bookings_monthly AS bookings_last_month
  FROM (
    -- Aggregate Measures
    SELECT
      subq_6.metric_time__month
      , SUM(subq_6.bookings_monthly) AS bookings_monthly
    FROM (
      -- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
      SELECT
        subq_5.metric_time__month
        , subq_5.bookings_monthly
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_1.ds__month AS ds__month
          , subq_1.ds__quarter AS ds__quarter
          , subq_1.ds__year AS ds__year
          , subq_1.ds__extract_year AS ds__extract_year
          , subq_1.ds__extract_quarter AS ds__extract_quarter
          , subq_1.ds__extract_month AS ds__extract_month
          , subq_1.booking_monthly__ds__month AS booking_monthly__ds__month
          , subq_1.booking_monthly__ds__quarter AS booking_monthly__ds__quarter
          , subq_1.booking_monthly__ds__year AS booking_monthly__ds__year
          , subq_1.booking_monthly__ds__extract_year AS booking_monthly__ds__extract_year
          , subq_1.booking_monthly__ds__extract_quarter AS booking_monthly__ds__extract_quarter
          , subq_1.booking_monthly__ds__extract_month AS booking_monthly__ds__extract_month
          , subq_1.metric_time__quarter AS metric_time__quarter
          , subq_1.metric_time__year AS metric_time__year
          , subq_1.metric_time__extract_year AS metric_time__extract_year
          , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_1.metric_time__extract_month AS metric_time__extract_month
          , subq_4.metric_time__month AS metric_time__month
          , subq_1.listing AS listing
          , subq_1.booking_monthly__listing AS booking_monthly__listing
          , subq_1.bookings_monthly AS bookings_monthly
        FROM (
          -- Pass Only Elements: ['metric_time__month', 'metric_time__month']
          SELECT
            subq_3.metric_time__month
          FROM (
            -- Change Column Aliases
            SELECT
              subq_2.ds__day
              , subq_2.ds__week
              , subq_2.ds__month AS metric_time__month
              , subq_2.ds__quarter
              , subq_2.ds__year
              , subq_2.ds__extract_year
              , subq_2.ds__extract_quarter
              , subq_2.ds__extract_month
              , subq_2.ds__extract_day
              , subq_2.ds__extract_dow
              , subq_2.ds__extract_doy
              , subq_2.ds__martian_day
            FROM (
              -- Read From Time Spine 'mf_time_spine'
              SELECT
                time_spine_src_16006.ds AS ds__day
                , DATE_TRUNC('week', time_spine_src_16006.ds) AS ds__week
                , DATE_TRUNC('month', time_spine_src_16006.ds) AS ds__month
                , DATE_TRUNC('quarter', time_spine_src_16006.ds) AS ds__quarter
                , DATE_TRUNC('year', time_spine_src_16006.ds) AS ds__year
                , EXTRACT(year FROM time_spine_src_16006.ds) AS ds__extract_year
                , EXTRACT(quarter FROM time_spine_src_16006.ds) AS ds__extract_quarter
                , EXTRACT(month FROM time_spine_src_16006.ds) AS ds__extract_month
                , EXTRACT(day FROM time_spine_src_16006.ds) AS ds__extract_day
                , EXTRACT(dayofweekiso FROM time_spine_src_16006.ds) AS ds__extract_dow
                , EXTRACT(doy FROM time_spine_src_16006.ds) AS ds__extract_doy
                , time_spine_src_16006.martian_day AS ds__martian_day
              FROM ***************************.mf_time_spine time_spine_src_16006
            ) subq_2
          ) subq_3
          GROUP BY
            subq_3.metric_time__month
        ) subq_4
        INNER JOIN (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.booking_monthly__ds__month
            , subq_0.booking_monthly__ds__quarter
            , subq_0.booking_monthly__ds__year
            , subq_0.booking_monthly__ds__extract_year
            , subq_0.booking_monthly__ds__extract_quarter
            , subq_0.booking_monthly__ds__extract_month
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.ds__extract_year AS metric_time__extract_year
            , subq_0.ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds__extract_month AS metric_time__extract_month
            , subq_0.listing
            , subq_0.booking_monthly__listing
            , subq_0.bookings_monthly
          FROM (
            -- Read Elements From Semantic Model 'monthly_bookings_source'
            SELECT
              monthly_bookings_source_src_16000.bookings_monthly
              , DATE_TRUNC('month', monthly_bookings_source_src_16000.ds) AS ds__month
              , DATE_TRUNC('quarter', monthly_bookings_source_src_16000.ds) AS ds__quarter
              , DATE_TRUNC('year', monthly_bookings_source_src_16000.ds) AS ds__year
              , EXTRACT(year FROM monthly_bookings_source_src_16000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM monthly_bookings_source_src_16000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM monthly_bookings_source_src_16000.ds) AS ds__extract_month
              , DATE_TRUNC('month', monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__month
              , DATE_TRUNC('quarter', monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__quarter
              , DATE_TRUNC('year', monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__year
              , EXTRACT(year FROM monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_year
              , EXTRACT(quarter FROM monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_quarter
              , EXTRACT(month FROM monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_month
              , monthly_bookings_source_src_16000.listing_id AS listing
              , monthly_bookings_source_src_16000.listing_id AS booking_monthly__listing
            FROM ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
          ) subq_0
        ) subq_1
        ON
          DATEADD(month, -1, subq_4.metric_time__month) = subq_1.metric_time__month
      ) subq_5
    ) subq_6
    GROUP BY
      subq_6.metric_time__month
  ) subq_7
) subq_8
