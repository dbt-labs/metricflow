-- Compute Metrics via Expressions
SELECT
  subq_5.metric_time__day
  , subq_5.bookings AS family_bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_4.metric_time__day
    , SUM(subq_4.bookings) AS bookings
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      subq_3.metric_time__day
      , subq_3.bookings
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['bookings', 'listing__capacity', 'metric_time__day']
      SELECT
        subq_1.metric_time__day AS metric_time__day
        , subq_1.bookings AS bookings
      FROM (
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
        SELECT
          subq_0.ds__day AS metric_time__day
          , subq_0.listing
          , subq_0.bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          SELECT
            1 AS bookings
            , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
            , bookings_source_src_26000.booking_value
            , bookings_source_src_26000.guest_id AS bookers
            , bookings_source_src_26000.booking_value AS average_booking_value
            , bookings_source_src_26000.booking_value AS booking_payments
            , bookings_source_src_26000.is_instant
            , DATE_TRUNC('day', bookings_source_src_26000.ds) AS ds__day
            , DATE_TRUNC('week', bookings_source_src_26000.ds) AS ds__week
            , DATE_TRUNC('month', bookings_source_src_26000.ds) AS ds__month
            , DATE_TRUNC('quarter', bookings_source_src_26000.ds) AS ds__quarter
            , DATE_TRUNC('year', bookings_source_src_26000.ds) AS ds__year
            , EXTRACT(year FROM bookings_source_src_26000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds) AS ds__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds) AS ds__extract_day
            , EXTRACT(isodow FROM bookings_source_src_26000.ds) AS ds__extract_dow
            , EXTRACT(doy FROM bookings_source_src_26000.ds) AS ds__extract_doy
            , DATE_TRUNC('day', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__day
            , DATE_TRUNC('week', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__week
            , DATE_TRUNC('month', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__month
            , DATE_TRUNC('quarter', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__quarter
            , DATE_TRUNC('year', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__year
            , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_day
            , EXTRACT(isodow FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_dow
            , EXTRACT(doy FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_doy
            , DATE_TRUNC('day', bookings_source_src_26000.paid_at) AS paid_at__day
            , DATE_TRUNC('week', bookings_source_src_26000.paid_at) AS paid_at__week
            , DATE_TRUNC('month', bookings_source_src_26000.paid_at) AS paid_at__month
            , DATE_TRUNC('quarter', bookings_source_src_26000.paid_at) AS paid_at__quarter
            , DATE_TRUNC('year', bookings_source_src_26000.paid_at) AS paid_at__year
            , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS paid_at__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS paid_at__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS paid_at__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS paid_at__extract_day
            , EXTRACT(isodow FROM bookings_source_src_26000.paid_at) AS paid_at__extract_dow
            , EXTRACT(doy FROM bookings_source_src_26000.paid_at) AS paid_at__extract_doy
            , bookings_source_src_26000.is_instant AS booking__is_instant
            , DATE_TRUNC('day', bookings_source_src_26000.ds) AS booking__ds__day
            , DATE_TRUNC('week', bookings_source_src_26000.ds) AS booking__ds__week
            , DATE_TRUNC('month', bookings_source_src_26000.ds) AS booking__ds__month
            , DATE_TRUNC('quarter', bookings_source_src_26000.ds) AS booking__ds__quarter
            , DATE_TRUNC('year', bookings_source_src_26000.ds) AS booking__ds__year
            , EXTRACT(year FROM bookings_source_src_26000.ds) AS booking__ds__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS booking__ds__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds) AS booking__ds__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds) AS booking__ds__extract_day
            , EXTRACT(isodow FROM bookings_source_src_26000.ds) AS booking__ds__extract_dow
            , EXTRACT(doy FROM bookings_source_src_26000.ds) AS booking__ds__extract_doy
            , DATE_TRUNC('day', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__day
            , DATE_TRUNC('week', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__week
            , DATE_TRUNC('month', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__month
            , DATE_TRUNC('quarter', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__quarter
            , DATE_TRUNC('year', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__year
            , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_day
            , EXTRACT(isodow FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_dow
            , EXTRACT(doy FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_doy
            , DATE_TRUNC('day', bookings_source_src_26000.paid_at) AS booking__paid_at__day
            , DATE_TRUNC('week', bookings_source_src_26000.paid_at) AS booking__paid_at__week
            , DATE_TRUNC('month', bookings_source_src_26000.paid_at) AS booking__paid_at__month
            , DATE_TRUNC('quarter', bookings_source_src_26000.paid_at) AS booking__paid_at__quarter
            , DATE_TRUNC('year', bookings_source_src_26000.paid_at) AS booking__paid_at__year
            , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_day
            , EXTRACT(isodow FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_dow
            , EXTRACT(doy FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_doy
            , bookings_source_src_26000.listing_id AS listing
            , bookings_source_src_26000.guest_id AS guest
            , bookings_source_src_26000.host_id AS host
            , bookings_source_src_26000.guest_id AS user
            , bookings_source_src_26000.listing_id AS booking__listing
            , bookings_source_src_26000.guest_id AS booking__guest
            , bookings_source_src_26000.host_id AS booking__host
            , bookings_source_src_26000.guest_id AS booking__user
          FROM ***************************.fct_bookings bookings_source_src_26000
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Read From SemanticModelDataSet('listings')
        -- Pass Only Elements: ['capacity', 'window_start__day', 'window_end__day', 'listing']
        SELECT
          listings_src_26000.active_from AS window_start__day
          , listings_src_26000.active_to AS window_end__day
          , listings_src_26000.capacity
          , listings_src_26000.listing_id AS listing
        FROM ***************************.dim_listings listings_src_26000
      ) subq_2
      ON
        (
          subq_1.listing = subq_2.listing
        ) AND (
          (
            subq_1.metric_time__day >= subq_2.window_start__day
          ) AND (
            (
              subq_1.metric_time__day < subq_2.window_end__day
            ) OR (
              subq_2.window_end__day IS NULL
            )
          )
        )
    ) subq_3
    WHERE listing__capacity > 2
  ) subq_4
  GROUP BY
    subq_4.metric_time__day
) subq_5
