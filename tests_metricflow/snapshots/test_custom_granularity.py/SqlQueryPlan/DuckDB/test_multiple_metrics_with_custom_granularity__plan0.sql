-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_4.metric_time__martian_day, subq_9.metric_time__martian_day) AS metric_time__martian_day
  , MAX(subq_4.bookings) AS bookings
  , MAX(subq_9.listings) AS listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_3.metric_time__martian_day
    , subq_3.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_2.metric_time__martian_day
      , SUM(subq_2.bookings) AS bookings
    FROM (
      -- Join to Custom Granularity Dataset
      -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
      SELECT
        subq_0.bookings AS bookings
        , subq_1.martian_day AS metric_time__martian_day
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS bookings
          , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
          , bookings_source_src_28000.booking_value
          , bookings_source_src_28000.booking_value AS max_booking_value
          , bookings_source_src_28000.booking_value AS min_booking_value
          , bookings_source_src_28000.guest_id AS bookers
          , bookings_source_src_28000.booking_value AS average_booking_value
          , bookings_source_src_28000.booking_value AS booking_payments
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
          , bookings_source_src_28000.booking_value AS median_booking_value
          , bookings_source_src_28000.booking_value AS booking_value_p99
          , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
          , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
          , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
          , bookings_source_src_28000.is_instant
          , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
          , DATE_TRUNC('week', bookings_source_src_28000.ds) AS ds__week
          , DATE_TRUNC('month', bookings_source_src_28000.ds) AS ds__month
          , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS ds__quarter
          , DATE_TRUNC('year', bookings_source_src_28000.ds) AS ds__year
          , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
          , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS ds__extract_dow
          , EXTRACT(doy FROM bookings_source_src_28000.ds) AS ds__extract_doy
          , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
          , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
          , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
          , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
          , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS paid_at__day
          , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS paid_at__week
          , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS paid_at__year
          , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
          , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
          , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
          , bookings_source_src_28000.is_instant AS booking__is_instant
          , DATE_TRUNC('day', bookings_source_src_28000.ds) AS booking__ds__day
          , DATE_TRUNC('week', bookings_source_src_28000.ds) AS booking__ds__week
          , DATE_TRUNC('month', bookings_source_src_28000.ds) AS booking__ds__month
          , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
          , DATE_TRUNC('year', bookings_source_src_28000.ds) AS booking__ds__year
          , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
          , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
          , EXTRACT(doy FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
          , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
          , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
          , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
          , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
          , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
          , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
          , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
          , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
          , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
          , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
          , bookings_source_src_28000.listing_id AS listing
          , bookings_source_src_28000.guest_id AS guest
          , bookings_source_src_28000.host_id AS host
          , bookings_source_src_28000.listing_id AS booking__listing
          , bookings_source_src_28000.guest_id AS booking__guest
          , bookings_source_src_28000.host_id AS booking__host
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_0
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_1
      ON
        subq_0.ds__day = subq_1.ds
    ) subq_2
    GROUP BY
      subq_2.metric_time__martian_day
  ) subq_3
) subq_4
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.metric_time__martian_day
    , subq_8.listings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_7.metric_time__martian_day
      , SUM(subq_7.listings) AS listings
    FROM (
      -- Join to Custom Granularity Dataset
      -- Pass Only Elements: ['listings', 'metric_time__martian_day']
      SELECT
        subq_5.listings AS listings
        , subq_6.martian_day AS metric_time__martian_day
      FROM (
        -- Read Elements From Semantic Model 'listings_latest'
        SELECT
          1 AS listings
          , listings_latest_src_28000.capacity AS largest_listing
          , listings_latest_src_28000.capacity AS smallest_listing
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
          , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
          , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
          , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
          , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
          , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS ds__extract_dow
          , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
          , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
          , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
          , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
          , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
          , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
          , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
          , listings_latest_src_28000.country AS country_latest
          , listings_latest_src_28000.is_lux AS is_lux_latest
          , listings_latest_src_28000.capacity AS capacity_latest
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
          , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
          , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
          , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
          , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
          , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
          , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
          , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
          , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
          , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
          , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
          , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
          , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
          , listings_latest_src_28000.country AS listing__country_latest
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , listings_latest_src_28000.capacity AS listing__capacity_latest
          , listings_latest_src_28000.listing_id AS listing
          , listings_latest_src_28000.user_id AS user
          , listings_latest_src_28000.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_28000
      ) subq_5
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_6
      ON
        subq_5.ds__day = subq_6.ds
    ) subq_7
    GROUP BY
      subq_7.metric_time__martian_day
  ) subq_8
) subq_9
ON
  subq_4.metric_time__martian_day = subq_9.metric_time__martian_day
GROUP BY
  COALESCE(subq_4.metric_time__martian_day, subq_9.metric_time__martian_day)
