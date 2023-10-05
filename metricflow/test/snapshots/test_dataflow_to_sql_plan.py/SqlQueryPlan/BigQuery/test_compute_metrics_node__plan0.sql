-- Compute Metrics via Expressions
SELECT
  subq_5.listing
  , subq_5.listing__country_latest
  , subq_5.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_4.listing
    , subq_4.listing__country_latest
    , SUM(subq_4.bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_1.listing AS listing
      , subq_3.country_latest AS listing__country_latest
      , subq_1.bookings AS bookings
    FROM (
      -- Pass Only Elements:
      --   ['bookings', 'listing']
      SELECT
        subq_0.listing
        , subq_0.bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS bookings
          , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
          , bookings_source_src_10001.booking_value
          , bookings_source_src_10001.booking_value AS max_booking_value
          , bookings_source_src_10001.booking_value AS min_booking_value
          , bookings_source_src_10001.guest_id AS bookers
          , bookings_source_src_10001.booking_value AS average_booking_value
          , bookings_source_src_10001.booking_value AS booking_payments
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
          , bookings_source_src_10001.booking_value AS median_booking_value
          , bookings_source_src_10001.booking_value AS booking_value_p99
          , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
          , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
          , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
          , bookings_source_src_10001.is_instant
          , DATE_TRUNC(bookings_source_src_10001.ds, day) AS ds__day
          , DATE_TRUNC(bookings_source_src_10001.ds, isoweek) AS ds__week
          , DATE_TRUNC(bookings_source_src_10001.ds, month) AS ds__month
          , DATE_TRUNC(bookings_source_src_10001.ds, quarter) AS ds__quarter
          , DATE_TRUNC(bookings_source_src_10001.ds, year) AS ds__year
          , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
          , EXTRACT(isoweek FROM bookings_source_src_10001.ds) AS ds__extract_week
          , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
          , EXTRACT(dayofweek FROM bookings_source_src_10001.ds) AS ds__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_10001.ds) AS ds__extract_doy
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, day) AS ds_partitioned__day
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoweek) AS ds_partitioned__week
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, month) AS ds_partitioned__month
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, quarter) AS ds_partitioned__quarter
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, year) AS ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
          , EXTRACT(isoweek FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_week
          , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
          , EXTRACT(dayofweek FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
          , DATE_TRUNC(bookings_source_src_10001.paid_at, day) AS paid_at__day
          , DATE_TRUNC(bookings_source_src_10001.paid_at, isoweek) AS paid_at__week
          , DATE_TRUNC(bookings_source_src_10001.paid_at, month) AS paid_at__month
          , DATE_TRUNC(bookings_source_src_10001.paid_at, quarter) AS paid_at__quarter
          , DATE_TRUNC(bookings_source_src_10001.paid_at, year) AS paid_at__year
          , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
          , EXTRACT(isoweek FROM bookings_source_src_10001.paid_at) AS paid_at__extract_week
          , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
          , EXTRACT(dayofweek FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
          , bookings_source_src_10001.is_instant AS booking__is_instant
          , DATE_TRUNC(bookings_source_src_10001.ds, day) AS booking__ds__day
          , DATE_TRUNC(bookings_source_src_10001.ds, isoweek) AS booking__ds__week
          , DATE_TRUNC(bookings_source_src_10001.ds, month) AS booking__ds__month
          , DATE_TRUNC(bookings_source_src_10001.ds, quarter) AS booking__ds__quarter
          , DATE_TRUNC(bookings_source_src_10001.ds, year) AS booking__ds__year
          , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
          , EXTRACT(isoweek FROM bookings_source_src_10001.ds) AS booking__ds__extract_week
          , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
          , EXTRACT(dayofweek FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, day) AS booking__ds_partitioned__day
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoweek) AS booking__ds_partitioned__week
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, month) AS booking__ds_partitioned__month
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
          , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, year) AS booking__ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
          , EXTRACT(isoweek FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_week
          , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
          , EXTRACT(dayofweek FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
          , DATE_TRUNC(bookings_source_src_10001.paid_at, day) AS booking__paid_at__day
          , DATE_TRUNC(bookings_source_src_10001.paid_at, isoweek) AS booking__paid_at__week
          , DATE_TRUNC(bookings_source_src_10001.paid_at, month) AS booking__paid_at__month
          , DATE_TRUNC(bookings_source_src_10001.paid_at, quarter) AS booking__paid_at__quarter
          , DATE_TRUNC(bookings_source_src_10001.paid_at, year) AS booking__paid_at__year
          , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
          , EXTRACT(isoweek FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_week
          , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
          , EXTRACT(dayofweek FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
          , bookings_source_src_10001.listing_id AS listing
          , bookings_source_src_10001.guest_id AS guest
          , bookings_source_src_10001.host_id AS host
          , bookings_source_src_10001.listing_id AS booking__listing
          , bookings_source_src_10001.guest_id AS booking__guest
          , bookings_source_src_10001.host_id AS booking__host
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_0
    ) subq_1
    LEFT OUTER JOIN (
      -- Pass Only Elements:
      --   ['country_latest', 'listing']
      SELECT
        subq_2.listing
        , subq_2.country_latest
      FROM (
        -- Read Elements From Semantic Model 'listings_latest'
        SELECT
          1 AS listings
          , listings_latest_src_10004.capacity AS largest_listing
          , listings_latest_src_10004.capacity AS smallest_listing
          , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS ds__day
          , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS ds__week
          , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS ds__month
          , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS ds__quarter
          , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS ds__year
          , EXTRACT(year FROM listings_latest_src_10004.created_at) AS ds__extract_year
          , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS ds__extract_quarter
          , EXTRACT(month FROM listings_latest_src_10004.created_at) AS ds__extract_month
          , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS ds__extract_week
          , EXTRACT(day FROM listings_latest_src_10004.created_at) AS ds__extract_day
          , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS ds__extract_dow
          , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS ds__extract_doy
          , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS created_at__day
          , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS created_at__week
          , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS created_at__month
          , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS created_at__quarter
          , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS created_at__year
          , EXTRACT(year FROM listings_latest_src_10004.created_at) AS created_at__extract_year
          , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS created_at__extract_quarter
          , EXTRACT(month FROM listings_latest_src_10004.created_at) AS created_at__extract_month
          , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS created_at__extract_week
          , EXTRACT(day FROM listings_latest_src_10004.created_at) AS created_at__extract_day
          , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS created_at__extract_dow
          , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS created_at__extract_doy
          , listings_latest_src_10004.country AS country_latest
          , listings_latest_src_10004.is_lux AS is_lux_latest
          , listings_latest_src_10004.capacity AS capacity_latest
          , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS listing__ds__day
          , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__ds__week
          , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__ds__month
          , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__ds__quarter
          , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS listing__ds__year
          , EXTRACT(year FROM listings_latest_src_10004.created_at) AS listing__ds__extract_year
          , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS listing__ds__extract_quarter
          , EXTRACT(month FROM listings_latest_src_10004.created_at) AS listing__ds__extract_month
          , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS listing__ds__extract_week
          , EXTRACT(day FROM listings_latest_src_10004.created_at) AS listing__ds__extract_day
          , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS listing__ds__extract_dow
          , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS listing__ds__extract_doy
          , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS listing__created_at__day
          , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__created_at__week
          , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__created_at__month
          , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__created_at__quarter
          , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS listing__created_at__year
          , EXTRACT(year FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_year
          , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_quarter
          , EXTRACT(month FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_month
          , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_week
          , EXTRACT(day FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_day
          , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_dow
          , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_doy
          , listings_latest_src_10004.country AS listing__country_latest
          , listings_latest_src_10004.is_lux AS listing__is_lux_latest
          , listings_latest_src_10004.capacity AS listing__capacity_latest
          , listings_latest_src_10004.listing_id AS listing
          , listings_latest_src_10004.user_id AS user
          , listings_latest_src_10004.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_10004
      ) subq_2
    ) subq_3
    ON
      subq_1.listing = subq_3.listing
  ) subq_4
  GROUP BY
    listing
    , listing__country_latest
) subq_5
