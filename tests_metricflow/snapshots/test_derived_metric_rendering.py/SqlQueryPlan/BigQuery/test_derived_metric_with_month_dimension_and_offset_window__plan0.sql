test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  subq_7.metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_6.metric_time__month
    , subq_6.bookings_monthly AS bookings_last_month
  FROM (
    -- Aggregate Measures
    SELECT
      subq_5.metric_time__month
      , SUM(subq_5.bookings_monthly) AS bookings_monthly
    FROM (
      -- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
      SELECT
        subq_4.metric_time__month
        , subq_4.bookings_monthly
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_2.metric_time__month AS metric_time__month
          , DATETIME_TRUNC(subq_2.metric_time__month, quarter) AS metric_time__quarter
          , DATETIME_TRUNC(subq_2.metric_time__month, year) AS metric_time__year
          , EXTRACT(year FROM subq_2.metric_time__month) AS metric_time__extract_year
          , EXTRACT(quarter FROM subq_2.metric_time__month) AS metric_time__extract_quarter
          , EXTRACT(month FROM subq_2.metric_time__month) AS metric_time__extract_month
          , subq_1.ds__month AS ds__month
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
          , subq_1.listing AS listing
          , subq_1.booking_monthly__listing AS booking_monthly__listing
          , subq_1.bookings_monthly AS bookings_monthly
        FROM (
          -- Time Spine
          SELECT
            DATETIME_TRUNC(subq_3.ds, month) AS metric_time__month
          FROM ***************************.mf_time_spine subq_3
          GROUP BY
            metric_time__month
        ) subq_2
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
              , DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, month) AS ds__month
              , DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, quarter) AS ds__quarter
              , DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, year) AS ds__year
              , EXTRACT(year FROM monthly_bookings_source_src_16000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM monthly_bookings_source_src_16000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM monthly_bookings_source_src_16000.ds) AS ds__extract_month
              , DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, month) AS booking_monthly__ds__month
              , DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, quarter) AS booking_monthly__ds__quarter
              , DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, year) AS booking_monthly__ds__year
              , EXTRACT(year FROM monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_year
              , EXTRACT(quarter FROM monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_quarter
              , EXTRACT(month FROM monthly_bookings_source_src_16000.ds) AS booking_monthly__ds__extract_month
              , monthly_bookings_source_src_16000.listing_id AS listing
              , monthly_bookings_source_src_16000.listing_id AS booking_monthly__listing
            FROM ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
          ) subq_0
        ) subq_1
        ON
          DATE_SUB(CAST(subq_2.metric_time__month AS DATETIME), INTERVAL 1 month) = subq_1.metric_time__month
      ) subq_4
    ) subq_5
    GROUP BY
      metric_time__month
  ) subq_6
) subq_7
