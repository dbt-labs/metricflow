test_name: test_no_metrics_with_custom_granularity_in_filter_and_group_by
test_filename: test_custom_granularity.py
docstring:
  Group by items only queried with a filter on a custom grain, where that grain is also used in the group by.
sql_engine: Clickhouse
---
-- Pass Only Elements: ['listing__ds__martian_day',]
SELECT
  subq_2.listing__ds__martian_day
FROM (
  -- Constrain Output with WHERE
  SELECT
    subq_1.listing__ds__martian_day
    , subq_1.ds__day
    , subq_1.ds__week
    , subq_1.ds__month
    , subq_1.ds__quarter
    , subq_1.ds__year
    , subq_1.ds__extract_year
    , subq_1.ds__extract_quarter
    , subq_1.ds__extract_month
    , subq_1.ds__extract_day
    , subq_1.ds__extract_dow
    , subq_1.ds__extract_doy
    , subq_1.created_at__day
    , subq_1.created_at__week
    , subq_1.created_at__month
    , subq_1.created_at__quarter
    , subq_1.created_at__year
    , subq_1.created_at__extract_year
    , subq_1.created_at__extract_quarter
    , subq_1.created_at__extract_month
    , subq_1.created_at__extract_day
    , subq_1.created_at__extract_dow
    , subq_1.created_at__extract_doy
    , subq_1.listing__ds__day
    , subq_1.listing__ds__week
    , subq_1.listing__ds__month
    , subq_1.listing__ds__quarter
    , subq_1.listing__ds__year
    , subq_1.listing__ds__extract_year
    , subq_1.listing__ds__extract_quarter
    , subq_1.listing__ds__extract_month
    , subq_1.listing__ds__extract_day
    , subq_1.listing__ds__extract_dow
    , subq_1.listing__ds__extract_doy
    , subq_1.listing__created_at__day
    , subq_1.listing__created_at__week
    , subq_1.listing__created_at__month
    , subq_1.listing__created_at__quarter
    , subq_1.listing__created_at__year
    , subq_1.listing__created_at__extract_year
    , subq_1.listing__created_at__extract_quarter
    , subq_1.listing__created_at__extract_month
    , subq_1.listing__created_at__extract_day
    , subq_1.listing__created_at__extract_dow
    , subq_1.listing__created_at__extract_doy
    , subq_1.listing
    , subq_1.user
    , subq_1.listing__user
    , subq_1.country_latest
    , subq_1.is_lux_latest
    , subq_1.capacity_latest
    , subq_1.listing__country_latest
    , subq_1.listing__is_lux_latest
    , subq_1.listing__capacity_latest
    , subq_1.listings
    , subq_1.largest_listing
    , subq_1.smallest_listing
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Join to Custom Granularity Dataset
    SELECT
      1 AS listings
      , listings_latest_src_28000.capacity AS largest_listing
      , listings_latest_src_28000.capacity AS smallest_listing
      , date_trunc('day', listings_latest_src_28000.created_at) AS ds__day
      , date_trunc('week', listings_latest_src_28000.created_at) AS ds__week
      , date_trunc('month', listings_latest_src_28000.created_at) AS ds__month
      , date_trunc('quarter', listings_latest_src_28000.created_at) AS ds__quarter
      , date_trunc('year', listings_latest_src_28000.created_at) AS ds__year
      , toYear(listings_latest_src_28000.created_at) AS ds__extract_year
      , toQuarter(listings_latest_src_28000.created_at) AS ds__extract_quarter
      , toMonth(listings_latest_src_28000.created_at) AS ds__extract_month
      , toDayOfMonth(listings_latest_src_28000.created_at) AS ds__extract_day
      , toDayOfWeek(listings_latest_src_28000.created_at) AS ds__extract_dow
      , toDayOfYear(listings_latest_src_28000.created_at) AS ds__extract_doy
      , date_trunc('day', listings_latest_src_28000.created_at) AS created_at__day
      , date_trunc('week', listings_latest_src_28000.created_at) AS created_at__week
      , date_trunc('month', listings_latest_src_28000.created_at) AS created_at__month
      , date_trunc('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
      , date_trunc('year', listings_latest_src_28000.created_at) AS created_at__year
      , toYear(listings_latest_src_28000.created_at) AS created_at__extract_year
      , toQuarter(listings_latest_src_28000.created_at) AS created_at__extract_quarter
      , toMonth(listings_latest_src_28000.created_at) AS created_at__extract_month
      , toDayOfMonth(listings_latest_src_28000.created_at) AS created_at__extract_day
      , toDayOfWeek(listings_latest_src_28000.created_at) AS created_at__extract_dow
      , toDayOfYear(listings_latest_src_28000.created_at) AS created_at__extract_doy
      , listings_latest_src_28000.country AS country_latest
      , listings_latest_src_28000.is_lux AS is_lux_latest
      , listings_latest_src_28000.capacity AS capacity_latest
      , date_trunc('day', listings_latest_src_28000.created_at) AS listing__ds__day
      , date_trunc('week', listings_latest_src_28000.created_at) AS listing__ds__week
      , date_trunc('month', listings_latest_src_28000.created_at) AS listing__ds__month
      , date_trunc('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
      , date_trunc('year', listings_latest_src_28000.created_at) AS listing__ds__year
      , toYear(listings_latest_src_28000.created_at) AS listing__ds__extract_year
      , toQuarter(listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
      , toMonth(listings_latest_src_28000.created_at) AS listing__ds__extract_month
      , toDayOfMonth(listings_latest_src_28000.created_at) AS listing__ds__extract_day
      , toDayOfWeek(listings_latest_src_28000.created_at) AS listing__ds__extract_dow
      , toDayOfYear(listings_latest_src_28000.created_at) AS listing__ds__extract_doy
      , date_trunc('day', listings_latest_src_28000.created_at) AS listing__created_at__day
      , date_trunc('week', listings_latest_src_28000.created_at) AS listing__created_at__week
      , date_trunc('month', listings_latest_src_28000.created_at) AS listing__created_at__month
      , date_trunc('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
      , date_trunc('year', listings_latest_src_28000.created_at) AS listing__created_at__year
      , toYear(listings_latest_src_28000.created_at) AS listing__created_at__extract_year
      , toQuarter(listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
      , toMonth(listings_latest_src_28000.created_at) AS listing__created_at__extract_month
      , toDayOfMonth(listings_latest_src_28000.created_at) AS listing__created_at__extract_day
      , toDayOfWeek(listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
      , toDayOfYear(listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
      , listings_latest_src_28000.country AS listing__country_latest
      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , listings_latest_src_28000.listing_id AS listing
      , listings_latest_src_28000.user_id AS user
      , listings_latest_src_28000.user_id AS listing__user
      , subq_0.martian_day AS listing__ds__martian_day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_0
    ON
      date_trunc('day', listings_latest_src_28000.created_at) = subq_0.ds
  ) subq_1
  WHERE (listing__ds__martian_day = '2020-01-01')
) subq_2
GROUP BY
  listing__ds__martian_day
