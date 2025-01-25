test_name: test_no_metrics_with_custom_granularity_filter
test_filename: test_custom_granularity.py
docstring:
  Group by items only queried with a filter on a custom grain, where that grain is not used in the group by.
sql_engine: Postgres
---
-- Pass Only Elements: ['listing__ds__day',]
SELECT
  nr_subq_2.listing__ds__day
FROM (
  -- Constrain Output with WHERE
  SELECT
    nr_subq_1.listing__ds__martian_day
    , nr_subq_1.ds__day
    , nr_subq_1.ds__week
    , nr_subq_1.ds__month
    , nr_subq_1.ds__quarter
    , nr_subq_1.ds__year
    , nr_subq_1.ds__extract_year
    , nr_subq_1.ds__extract_quarter
    , nr_subq_1.ds__extract_month
    , nr_subq_1.ds__extract_day
    , nr_subq_1.ds__extract_dow
    , nr_subq_1.ds__extract_doy
    , nr_subq_1.created_at__day
    , nr_subq_1.created_at__week
    , nr_subq_1.created_at__month
    , nr_subq_1.created_at__quarter
    , nr_subq_1.created_at__year
    , nr_subq_1.created_at__extract_year
    , nr_subq_1.created_at__extract_quarter
    , nr_subq_1.created_at__extract_month
    , nr_subq_1.created_at__extract_day
    , nr_subq_1.created_at__extract_dow
    , nr_subq_1.created_at__extract_doy
    , nr_subq_1.listing__ds__day
    , nr_subq_1.listing__ds__week
    , nr_subq_1.listing__ds__month
    , nr_subq_1.listing__ds__quarter
    , nr_subq_1.listing__ds__year
    , nr_subq_1.listing__ds__extract_year
    , nr_subq_1.listing__ds__extract_quarter
    , nr_subq_1.listing__ds__extract_month
    , nr_subq_1.listing__ds__extract_day
    , nr_subq_1.listing__ds__extract_dow
    , nr_subq_1.listing__ds__extract_doy
    , nr_subq_1.listing__created_at__day
    , nr_subq_1.listing__created_at__week
    , nr_subq_1.listing__created_at__month
    , nr_subq_1.listing__created_at__quarter
    , nr_subq_1.listing__created_at__year
    , nr_subq_1.listing__created_at__extract_year
    , nr_subq_1.listing__created_at__extract_quarter
    , nr_subq_1.listing__created_at__extract_month
    , nr_subq_1.listing__created_at__extract_day
    , nr_subq_1.listing__created_at__extract_dow
    , nr_subq_1.listing__created_at__extract_doy
    , nr_subq_1.listing
    , nr_subq_1.user
    , nr_subq_1.listing__user
    , nr_subq_1.country_latest
    , nr_subq_1.is_lux_latest
    , nr_subq_1.capacity_latest
    , nr_subq_1.listing__country_latest
    , nr_subq_1.listing__is_lux_latest
    , nr_subq_1.listing__capacity_latest
    , nr_subq_1.listings
    , nr_subq_1.largest_listing
    , nr_subq_1.smallest_listing
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Join to Custom Granularity Dataset
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
      , nr_subq_0.martian_day AS listing__ds__martian_day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
    LEFT OUTER JOIN
      ***************************.mf_time_spine nr_subq_0
    ON
      DATE_TRUNC('day', listings_latest_src_28000.created_at) = nr_subq_0.ds
  ) nr_subq_1
  WHERE listing__ds__martian_day = '2020-01-01'
) nr_subq_2
GROUP BY
  nr_subq_2.listing__ds__day
