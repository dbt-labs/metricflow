-- Compute Metrics via Expressions
SELECT
  subq_6.metric_time__day
  , subq_6.listing__lux_listing__is_confirmed_lux
  , subq_6.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_5.metric_time__day
    , subq_5.listing__lux_listing__is_confirmed_lux
    , SUM(subq_5.bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
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
      -- Join Standard Outputs
      -- Pass Only Elements: ['lux_listing__is_confirmed_lux', 'lux_listing__window_start__day', 'lux_listing__window_end__day', 'listing']
      SELECT
        subq_2.listing AS listing
      FROM (
        -- Read Elements From Semantic Model 'lux_listing_mapping'
        SELECT
          lux_listing_mapping_src_26000.listing_id AS listing
          , lux_listing_mapping_src_26000.lux_listing_id AS lux_listing
          , lux_listing_mapping_src_26000.lux_listing_id AS listing__lux_listing
        FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_26000
      ) subq_2
      LEFT OUTER JOIN (
        -- Read From SemanticModelDataSet('lux_listings')
        -- Pass Only Elements: [
        --   'is_confirmed_lux',
        --   'lux_listing__is_confirmed_lux',
        --   'window_start__day',
        --   'window_start__week',
        --   'window_start__month',
        --   'window_start__quarter',
        --   'window_start__year',
        --   'window_start__extract_year',
        --   'window_start__extract_quarter',
        --   'window_start__extract_month',
        --   'window_start__extract_day',
        --   'window_start__extract_dow',
        --   'window_start__extract_doy',
        --   'window_end__day',
        --   'window_end__week',
        --   'window_end__month',
        --   'window_end__quarter',
        --   'window_end__year',
        --   'window_end__extract_year',
        --   'window_end__extract_quarter',
        --   'window_end__extract_month',
        --   'window_end__extract_day',
        --   'window_end__extract_dow',
        --   'window_end__extract_doy',
        --   'lux_listing__window_start__day',
        --   'lux_listing__window_start__week',
        --   'lux_listing__window_start__month',
        --   'lux_listing__window_start__quarter',
        --   'lux_listing__window_start__year',
        --   'lux_listing__window_start__extract_year',
        --   'lux_listing__window_start__extract_quarter',
        --   'lux_listing__window_start__extract_month',
        --   'lux_listing__window_start__extract_day',
        --   'lux_listing__window_start__extract_dow',
        --   'lux_listing__window_start__extract_doy',
        --   'lux_listing__window_end__day',
        --   'lux_listing__window_end__week',
        --   'lux_listing__window_end__month',
        --   'lux_listing__window_end__quarter',
        --   'lux_listing__window_end__year',
        --   'lux_listing__window_end__extract_year',
        --   'lux_listing__window_end__extract_quarter',
        --   'lux_listing__window_end__extract_month',
        --   'lux_listing__window_end__extract_day',
        --   'lux_listing__window_end__extract_dow',
        --   'lux_listing__window_end__extract_doy',
        --   'lux_listing',
        -- ]
        SELECT
          lux_listings_src_26000.valid_from AS window_start__day
          , DATE_TRUNC('week', lux_listings_src_26000.valid_from) AS window_start__week
          , DATE_TRUNC('month', lux_listings_src_26000.valid_from) AS window_start__month
          , DATE_TRUNC('quarter', lux_listings_src_26000.valid_from) AS window_start__quarter
          , DATE_TRUNC('year', lux_listings_src_26000.valid_from) AS window_start__year
          , EXTRACT(year FROM lux_listings_src_26000.valid_from) AS window_start__extract_year
          , EXTRACT(quarter FROM lux_listings_src_26000.valid_from) AS window_start__extract_quarter
          , EXTRACT(month FROM lux_listings_src_26000.valid_from) AS window_start__extract_month
          , EXTRACT(day FROM lux_listings_src_26000.valid_from) AS window_start__extract_day
          , EXTRACT(isodow FROM lux_listings_src_26000.valid_from) AS window_start__extract_dow
          , EXTRACT(doy FROM lux_listings_src_26000.valid_from) AS window_start__extract_doy
          , lux_listings_src_26000.valid_to AS window_end__day
          , DATE_TRUNC('week', lux_listings_src_26000.valid_to) AS window_end__week
          , DATE_TRUNC('month', lux_listings_src_26000.valid_to) AS window_end__month
          , DATE_TRUNC('quarter', lux_listings_src_26000.valid_to) AS window_end__quarter
          , DATE_TRUNC('year', lux_listings_src_26000.valid_to) AS window_end__year
          , EXTRACT(year FROM lux_listings_src_26000.valid_to) AS window_end__extract_year
          , EXTRACT(quarter FROM lux_listings_src_26000.valid_to) AS window_end__extract_quarter
          , EXTRACT(month FROM lux_listings_src_26000.valid_to) AS window_end__extract_month
          , EXTRACT(day FROM lux_listings_src_26000.valid_to) AS window_end__extract_day
          , EXTRACT(isodow FROM lux_listings_src_26000.valid_to) AS window_end__extract_dow
          , EXTRACT(doy FROM lux_listings_src_26000.valid_to) AS window_end__extract_doy
          , lux_listings_src_26000.is_confirmed_lux
          , lux_listings_src_26000.valid_from AS lux_listing__window_start__day
          , DATE_TRUNC('week', lux_listings_src_26000.valid_from) AS lux_listing__window_start__week
          , DATE_TRUNC('month', lux_listings_src_26000.valid_from) AS lux_listing__window_start__month
          , DATE_TRUNC('quarter', lux_listings_src_26000.valid_from) AS lux_listing__window_start__quarter
          , DATE_TRUNC('year', lux_listings_src_26000.valid_from) AS lux_listing__window_start__year
          , EXTRACT(year FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_year
          , EXTRACT(quarter FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_quarter
          , EXTRACT(month FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_month
          , EXTRACT(day FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_day
          , EXTRACT(isodow FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_dow
          , EXTRACT(doy FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_doy
          , lux_listings_src_26000.valid_to AS lux_listing__window_end__day
          , DATE_TRUNC('week', lux_listings_src_26000.valid_to) AS lux_listing__window_end__week
          , DATE_TRUNC('month', lux_listings_src_26000.valid_to) AS lux_listing__window_end__month
          , DATE_TRUNC('quarter', lux_listings_src_26000.valid_to) AS lux_listing__window_end__quarter
          , DATE_TRUNC('year', lux_listings_src_26000.valid_to) AS lux_listing__window_end__year
          , EXTRACT(year FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_year
          , EXTRACT(quarter FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_quarter
          , EXTRACT(month FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_month
          , EXTRACT(day FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_day
          , EXTRACT(isodow FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_dow
          , EXTRACT(doy FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_doy
          , lux_listings_src_26000.is_confirmed_lux AS lux_listing__is_confirmed_lux
          , lux_listings_src_26000.lux_listing_id AS lux_listing
        FROM ***************************.dim_lux_listings lux_listings_src_26000
      ) subq_3
      ON
        subq_2.lux_listing = subq_3.lux_listing
    ) subq_4
    ON
      (
        subq_1.listing = subq_4.listing
      ) AND (
        (
          subq_1.metric_time__day >= subq_4.window_start__day
        ) AND (
          (
            subq_1.metric_time__day < subq_4.window_end__day
          ) OR (
            subq_4.window_end__day IS NULL
          )
        )
      )
  ) subq_5
  GROUP BY
    subq_5.metric_time__day
    , subq_5.listing__lux_listing__is_confirmed_lux
) subq_6
