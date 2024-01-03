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
      -- Pass Only Elements:
      --   ['bookings_monthly', 'metric_time__month']
      SELECT
        subq_4.metric_time__month
        , subq_4.bookings_monthly
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_2.metric_time__month AS metric_time__month
          , subq_1.monthly_ds__month AS monthly_ds__month
          , subq_1.monthly_ds__quarter AS monthly_ds__quarter
          , subq_1.monthly_ds__year AS monthly_ds__year
          , subq_1.monthly_ds__extract_year AS monthly_ds__extract_year
          , subq_1.monthly_ds__extract_quarter AS monthly_ds__extract_quarter
          , subq_1.monthly_ds__extract_month AS monthly_ds__extract_month
          , subq_1.booking__monthly_ds__month AS booking__monthly_ds__month
          , subq_1.booking__monthly_ds__quarter AS booking__monthly_ds__quarter
          , subq_1.booking__monthly_ds__year AS booking__monthly_ds__year
          , subq_1.booking__monthly_ds__extract_year AS booking__monthly_ds__extract_year
          , subq_1.booking__monthly_ds__extract_quarter AS booking__monthly_ds__extract_quarter
          , subq_1.booking__monthly_ds__extract_month AS booking__monthly_ds__extract_month
          , subq_1.listing AS listing
          , subq_1.booking__listing AS booking__listing
          , subq_1.bookings_monthly AS bookings_monthly
        FROM (
          -- Date Spine
          SELECT
            DATE_TRUNC('month', subq_3.ds) AS metric_time__month
          FROM ***************************.mf_time_spine subq_3
          GROUP BY
            DATE_TRUNC('month', subq_3.ds)
        ) subq_2
        INNER JOIN (
          -- Metric Time Dimension 'monthly_ds'
          SELECT
            subq_0.monthly_ds__month
            , subq_0.monthly_ds__quarter
            , subq_0.monthly_ds__year
            , subq_0.monthly_ds__extract_year
            , subq_0.monthly_ds__extract_quarter
            , subq_0.monthly_ds__extract_month
            , subq_0.booking__monthly_ds__month
            , subq_0.booking__monthly_ds__quarter
            , subq_0.booking__monthly_ds__year
            , subq_0.booking__monthly_ds__extract_year
            , subq_0.booking__monthly_ds__extract_quarter
            , subq_0.booking__monthly_ds__extract_month
            , subq_0.monthly_ds__month AS metric_time__month
            , subq_0.monthly_ds__quarter AS metric_time__quarter
            , subq_0.monthly_ds__year AS metric_time__year
            , subq_0.monthly_ds__extract_year AS metric_time__extract_year
            , subq_0.monthly_ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.monthly_ds__extract_month AS metric_time__extract_month
            , subq_0.listing
            , subq_0.booking__listing
            , subq_0.bookings_monthly
          FROM (
            -- Read Elements From Semantic Model 'bookings_monthly_source'
            SELECT
              bookings_monthly_source_src_10026.bookings_monthly
              , DATE_TRUNC('month', bookings_monthly_source_src_10026.ds) AS monthly_ds__month
              , DATE_TRUNC('quarter', bookings_monthly_source_src_10026.ds) AS monthly_ds__quarter
              , DATE_TRUNC('year', bookings_monthly_source_src_10026.ds) AS monthly_ds__year
              , EXTRACT(year FROM bookings_monthly_source_src_10026.ds) AS monthly_ds__extract_year
              , EXTRACT(quarter FROM bookings_monthly_source_src_10026.ds) AS monthly_ds__extract_quarter
              , EXTRACT(month FROM bookings_monthly_source_src_10026.ds) AS monthly_ds__extract_month
              , DATE_TRUNC('month', bookings_monthly_source_src_10026.ds) AS booking__monthly_ds__month
              , DATE_TRUNC('quarter', bookings_monthly_source_src_10026.ds) AS booking__monthly_ds__quarter
              , DATE_TRUNC('year', bookings_monthly_source_src_10026.ds) AS booking__monthly_ds__year
              , EXTRACT(year FROM bookings_monthly_source_src_10026.ds) AS booking__monthly_ds__extract_year
              , EXTRACT(quarter FROM bookings_monthly_source_src_10026.ds) AS booking__monthly_ds__extract_quarter
              , EXTRACT(month FROM bookings_monthly_source_src_10026.ds) AS booking__monthly_ds__extract_month
              , bookings_monthly_source_src_10026.listing_id AS listing
              , bookings_monthly_source_src_10026.listing_id AS booking__listing
            FROM ***************************.fct_bookings_extended_monthly bookings_monthly_source_src_10026
          ) subq_0
        ) subq_1
        ON
          DATE_ADD('month', -1, subq_2.metric_time__month) = subq_1.metric_time__month
      ) subq_4
    ) subq_5
    GROUP BY
      subq_5.metric_time__month
  ) subq_6
) subq_7
