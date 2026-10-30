test_name: test_join_to_base_on_multiple_shared_group_bys
test_filename: test_join_to_base_aggregated_outputs_to_sql.py
sql_engine: DuckDB
---
-- Join Aggregated Outputs to Base
SELECT
  subq_2.listing__country_latest AS listing__country_latest
  , subq_2.listing__is_lux_latest AS listing__is_lux_latest
  , subq_2.listing__capacity_latest AS listing__capacity_latest
  , subq_2.__largest_listing AS __largest_listing
  , subq_5.__listings AS __listings
FROM (
  -- Aggregate Inputs for Simple Metrics
  SELECT
    subq_1.listing__country_latest
    , subq_1.listing__is_lux_latest
    , subq_1.listing__capacity_latest
    , MAX(subq_1.__largest_listing) AS __largest_listing
  FROM (
    -- Select: ['__largest_listing', 'listing__country_latest', 'listing__is_lux_latest', 'listing__capacity_latest']
    SELECT
      subq_0.listing__country_latest
      , subq_0.listing__is_lux_latest
      , subq_0.listing__capacity_latest
      , subq_0.__largest_listing
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      SELECT
        1 AS __listings
        , 1 AS __lux_listings
        , listings_latest_src_28000.capacity AS __smallest_listing
        , listings_latest_src_28000.capacity AS __largest_listing
        , 1 AS __active_listings
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
    ) subq_0
  ) subq_1
  GROUP BY
    subq_1.listing__country_latest
    , subq_1.listing__is_lux_latest
    , subq_1.listing__capacity_latest
) subq_2
LEFT OUTER JOIN (
  -- Aggregate Inputs for Simple Metrics
  SELECT
    subq_4.listing__country_latest
    , subq_4.listing__is_lux_latest
    , SUM(subq_4.__listings) AS __listings
  FROM (
    -- Select: ['__listings', 'listing__country_latest', 'listing__is_lux_latest']
    SELECT
      subq_3.listing__country_latest
      , subq_3.listing__is_lux_latest
      , subq_3.__listings
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      SELECT
        1 AS __listings
        , 1 AS __lux_listings
        , listings_latest_src_28000.capacity AS __smallest_listing
        , listings_latest_src_28000.capacity AS __largest_listing
        , 1 AS __active_listings
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
    ) subq_3
  ) subq_4
  GROUP BY
    subq_4.listing__country_latest
    , subq_4.listing__is_lux_latest
) subq_5
ON
  (
    (
      subq_2.listing__country_latest = subq_5.listing__country_latest
    ) OR (
      (
        subq_2.listing__country_latest IS NULL
      ) AND (
        subq_5.listing__country_latest IS NULL
      )
    )
  ) AND (
    (
      subq_2.listing__is_lux_latest = subq_5.listing__is_lux_latest
    ) OR (
      (
        subq_2.listing__is_lux_latest IS NULL
      ) AND (
        subq_5.listing__is_lux_latest IS NULL
      )
    )
  )
