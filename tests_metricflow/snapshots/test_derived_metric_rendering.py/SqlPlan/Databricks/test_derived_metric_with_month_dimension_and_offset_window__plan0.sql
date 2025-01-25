test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_7.metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Compute Metrics via Expressions
  SELECT
    nr_subq_6.metric_time__month
    , nr_subq_6.bookings_monthly AS bookings_last_month
  FROM (
    -- Aggregate Measures
    SELECT
      nr_subq_5.metric_time__month
      , SUM(nr_subq_5.bookings_monthly) AS bookings_monthly
    FROM (
      -- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
      SELECT
        nr_subq_4.metric_time__month
        , nr_subq_4.bookings_monthly
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          nr_subq_3.metric_time__month AS metric_time__month
          , nr_subq_0.ds__month AS ds__month
          , nr_subq_0.ds__quarter AS ds__quarter
          , nr_subq_0.ds__year AS ds__year
          , nr_subq_0.ds__extract_year AS ds__extract_year
          , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_0.ds__extract_month AS ds__extract_month
          , nr_subq_0.booking_monthly__ds__month AS booking_monthly__ds__month
          , nr_subq_0.booking_monthly__ds__quarter AS booking_monthly__ds__quarter
          , nr_subq_0.booking_monthly__ds__year AS booking_monthly__ds__year
          , nr_subq_0.booking_monthly__ds__extract_year AS booking_monthly__ds__extract_year
          , nr_subq_0.booking_monthly__ds__extract_quarter AS booking_monthly__ds__extract_quarter
          , nr_subq_0.booking_monthly__ds__extract_month AS booking_monthly__ds__extract_month
          , nr_subq_0.metric_time__quarter AS metric_time__quarter
          , nr_subq_0.metric_time__year AS metric_time__year
          , nr_subq_0.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_0.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_0.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_0.listing AS listing
          , nr_subq_0.booking_monthly__listing AS booking_monthly__listing
          , nr_subq_0.bookings_monthly AS bookings_monthly
        FROM (
          -- Pass Only Elements: ['metric_time__month',]
          SELECT
            nr_subq_2.metric_time__month
          FROM (
            -- Change Column Aliases
            SELECT
              nr_subq_1.ds__day
              , nr_subq_1.ds__week
              , nr_subq_1.ds__month AS metric_time__month
              , nr_subq_1.ds__quarter
              , nr_subq_1.ds__year
              , nr_subq_1.ds__extract_year
              , nr_subq_1.ds__extract_quarter
              , nr_subq_1.ds__extract_month
              , nr_subq_1.ds__extract_day
              , nr_subq_1.ds__extract_dow
              , nr_subq_1.ds__extract_doy
              , nr_subq_1.ds__martian_day
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
                , EXTRACT(DAYOFWEEK_ISO FROM time_spine_src_16006.ds) AS ds__extract_dow
                , EXTRACT(doy FROM time_spine_src_16006.ds) AS ds__extract_doy
                , time_spine_src_16006.martian_day AS ds__martian_day
              FROM ***************************.mf_time_spine time_spine_src_16006
            ) nr_subq_1
          ) nr_subq_2
          GROUP BY
            nr_subq_2.metric_time__month
        ) nr_subq_3
        INNER JOIN (
          -- Metric Time Dimension 'ds'
          SELECT
            nr_subq_16001.ds__month
            , nr_subq_16001.ds__quarter
            , nr_subq_16001.ds__year
            , nr_subq_16001.ds__extract_year
            , nr_subq_16001.ds__extract_quarter
            , nr_subq_16001.ds__extract_month
            , nr_subq_16001.booking_monthly__ds__month
            , nr_subq_16001.booking_monthly__ds__quarter
            , nr_subq_16001.booking_monthly__ds__year
            , nr_subq_16001.booking_monthly__ds__extract_year
            , nr_subq_16001.booking_monthly__ds__extract_quarter
            , nr_subq_16001.booking_monthly__ds__extract_month
            , nr_subq_16001.ds__month AS metric_time__month
            , nr_subq_16001.ds__quarter AS metric_time__quarter
            , nr_subq_16001.ds__year AS metric_time__year
            , nr_subq_16001.ds__extract_year AS metric_time__extract_year
            , nr_subq_16001.ds__extract_quarter AS metric_time__extract_quarter
            , nr_subq_16001.ds__extract_month AS metric_time__extract_month
            , nr_subq_16001.listing
            , nr_subq_16001.booking_monthly__listing
            , nr_subq_16001.bookings_monthly
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
          ) nr_subq_16001
        ) nr_subq_0
        ON
          DATEADD(month, -1, nr_subq_3.metric_time__month) = nr_subq_0.metric_time__month
      ) nr_subq_4
    ) nr_subq_5
    GROUP BY
      nr_subq_5.metric_time__month
  ) nr_subq_6
) nr_subq_7
