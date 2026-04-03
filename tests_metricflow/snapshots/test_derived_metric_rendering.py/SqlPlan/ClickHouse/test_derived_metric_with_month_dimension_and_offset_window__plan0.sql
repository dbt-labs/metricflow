test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_11.metric_time__month
  , subq_11.bookings_last_month
FROM (
  SELECT
    subq_10.metric_time__month
    , bookings_last_month AS bookings_last_month
  FROM (
    SELECT
      subq_9.metric_time__month
      , subq_9.__bookings_monthly AS bookings_last_month
    FROM (
      SELECT
        subq_8.metric_time__month AS metric_time__month
        , subq_4.__bookings_monthly AS __bookings_monthly
      FROM (
        SELECT
          subq_7.metric_time__month
        FROM (
          SELECT
            subq_6.metric_time__month
          FROM (
            SELECT
              subq_5.ds__day
              , subq_5.ds__week
              , subq_5.ds__month AS metric_time__month
              , subq_5.ds__quarter
              , subq_5.ds__year
              , subq_5.ds__extract_year
              , subq_5.ds__extract_quarter
              , subq_5.ds__extract_month
              , subq_5.ds__extract_day
              , subq_5.ds__extract_dow
              , subq_5.ds__extract_doy
              , subq_5.ds__alien_day
            FROM (
              SELECT
                time_spine_src_16006.ds AS ds__day
                , toStartOfWeek(time_spine_src_16006.ds, 1) AS ds__week
                , toStartOfMonth(time_spine_src_16006.ds) AS ds__month
                , toStartOfQuarter(time_spine_src_16006.ds) AS ds__quarter
                , toStartOfYear(time_spine_src_16006.ds) AS ds__year
                , toYear(time_spine_src_16006.ds) AS ds__extract_year
                , toQuarter(time_spine_src_16006.ds) AS ds__extract_quarter
                , toMonth(time_spine_src_16006.ds) AS ds__extract_month
                , toDayOfMonth(time_spine_src_16006.ds) AS ds__extract_day
                , toDayOfWeek(time_spine_src_16006.ds) AS ds__extract_dow
                , toDayOfYear(time_spine_src_16006.ds) AS ds__extract_doy
                , time_spine_src_16006.alien_day AS ds__alien_day
              FROM ***************************.mf_time_spine time_spine_src_16006
            ) subq_5
          ) subq_6
        ) subq_7
        GROUP BY
          subq_7.metric_time__month
      ) subq_8
      INNER JOIN (
        SELECT
          subq_3.metric_time__month
          , SUM(subq_3.__bookings_monthly) AS __bookings_monthly
        FROM (
          SELECT
            subq_2.metric_time__month
            , subq_2.__bookings_monthly
          FROM (
            SELECT
              subq_1.metric_time__month
              , subq_1.__bookings_monthly
            FROM (
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
                , subq_0.__bookings_monthly
              FROM (
                SELECT
                  monthly_bookings_source_src_16000.bookings_monthly AS __bookings_monthly
                  , toStartOfMonth(monthly_bookings_source_src_16000.ds) AS ds__month
                  , toStartOfQuarter(monthly_bookings_source_src_16000.ds) AS ds__quarter
                  , toStartOfYear(monthly_bookings_source_src_16000.ds) AS ds__year
                  , toYear(monthly_bookings_source_src_16000.ds) AS ds__extract_year
                  , toQuarter(monthly_bookings_source_src_16000.ds) AS ds__extract_quarter
                  , toMonth(monthly_bookings_source_src_16000.ds) AS ds__extract_month
                  , toStartOfMonth(monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__month
                  , toStartOfQuarter(monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__quarter
                  , toStartOfYear(monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__year
                  , toYear(monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_year
                  , toQuarter(monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_quarter
                  , toMonth(monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_month
                  , monthly_bookings_source_src_16000.listing_id AS listing
                  , monthly_bookings_source_src_16000.listing_id AS booking_monthly__listing
                FROM ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
              ) subq_0
            ) subq_1
          ) subq_2
        ) subq_3
        GROUP BY
          subq_3.metric_time__month
      ) subq_4
      ON
        addMonths(subq_8.metric_time__month, -1) = subq_4.metric_time__month
    ) subq_9
  ) subq_10
) subq_11
