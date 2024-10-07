-- Compute Metrics via Expressions
SELECT
  subq_8.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_7.listings) AS listings
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_6.listings
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['listings', 'user__revenue_all_time']
      SELECT
        subq_5.user__revenue_all_time AS user__revenue_all_time
        , subq_1.listings AS listings
      FROM (
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['listings', 'user']
        SELECT
          subq_0.user
          , subq_0.listings
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
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Compute Metrics via Expressions
        -- Pass Only Elements: ['user', 'user__revenue_all_time']
        SELECT
          subq_4.user
          , subq_4.txn_revenue AS user__revenue_all_time
        FROM (
          -- Aggregate Measures
          SELECT
            subq_3.user
            , SUM(subq_3.txn_revenue) AS txn_revenue
          FROM (
            -- Metric Time Dimension 'ds'
            -- Pass Only Elements: ['txn_revenue', 'user']
            SELECT
              subq_2.user
              , subq_2.txn_revenue
            FROM (
              -- Read Elements From Semantic Model 'revenue'
              SELECT
                revenue_src_28000.revenue AS txn_revenue
                , DATE_TRUNC('day', revenue_src_28000.created_at) AS ds__day
                , DATE_TRUNC('week', revenue_src_28000.created_at) AS ds__week
                , DATE_TRUNC('month', revenue_src_28000.created_at) AS ds__month
                , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS ds__quarter
                , DATE_TRUNC('year', revenue_src_28000.created_at) AS ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                , EXTRACT(isodow FROM revenue_src_28000.created_at) AS ds__extract_dow
                , EXTRACT(doy FROM revenue_src_28000.created_at) AS ds__extract_doy
                , DATE_TRUNC('day', revenue_src_28000.created_at) AS revenue_instance__ds__day
                , DATE_TRUNC('week', revenue_src_28000.created_at) AS revenue_instance__ds__week
                , DATE_TRUNC('month', revenue_src_28000.created_at) AS revenue_instance__ds__month
                , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS revenue_instance__ds__quarter
                , DATE_TRUNC('year', revenue_src_28000.created_at) AS revenue_instance__ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                , EXTRACT(isodow FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
                , EXTRACT(doy FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                , revenue_src_28000.user_id AS user
                , revenue_src_28000.user_id AS revenue_instance__user
              FROM ***************************.fct_revenue revenue_src_28000
            ) subq_2
          ) subq_3
          GROUP BY
            subq_3.user
        ) subq_4
      ) subq_5
      ON
        subq_1.user = subq_5.user
    ) subq_6
    WHERE user__revenue_all_time > 1
  ) subq_7
) subq_8
