test_name: test_no_dedupe
test_filename: test_query_rendering.py
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_9.metric_time__month
  , subq_9.listing__capacity
FROM (
  -- Pass Only Elements: ['listing__capacity', 'metric_time__month']
  SELECT
    subq_8.metric_time__month
    , subq_8.listing__capacity
  FROM (
    -- Constrain Output with WHERE
    SELECT
      subq_7.listing__capacity
      , subq_7.user__home_state_latest
      , subq_7.metric_time__month
    FROM (
      -- Pass Only Elements: ['listing__capacity', 'user__home_state_latest', 'metric_time__month']
      SELECT
        subq_6.metric_time__month
        , subq_6.listing__capacity
        , subq_6.user__home_state_latest
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_5.home_state_latest AS user__home_state_latest
          , subq_0.window_start__day AS window_start__day
          , subq_0.window_start__week AS window_start__week
          , subq_0.window_start__month AS window_start__month
          , subq_0.window_start__quarter AS window_start__quarter
          , subq_0.window_start__year AS window_start__year
          , subq_0.window_start__extract_year AS window_start__extract_year
          , subq_0.window_start__extract_quarter AS window_start__extract_quarter
          , subq_0.window_start__extract_month AS window_start__extract_month
          , subq_0.window_start__extract_day AS window_start__extract_day
          , subq_0.window_start__extract_dow AS window_start__extract_dow
          , subq_0.window_start__extract_doy AS window_start__extract_doy
          , subq_0.window_end__day AS window_end__day
          , subq_0.window_end__week AS window_end__week
          , subq_0.window_end__month AS window_end__month
          , subq_0.window_end__quarter AS window_end__quarter
          , subq_0.window_end__year AS window_end__year
          , subq_0.window_end__extract_year AS window_end__extract_year
          , subq_0.window_end__extract_quarter AS window_end__extract_quarter
          , subq_0.window_end__extract_month AS window_end__extract_month
          , subq_0.window_end__extract_day AS window_end__extract_day
          , subq_0.window_end__extract_dow AS window_end__extract_dow
          , subq_0.window_end__extract_doy AS window_end__extract_doy
          , subq_0.listing__window_start__day AS listing__window_start__day
          , subq_0.listing__window_start__week AS listing__window_start__week
          , subq_0.listing__window_start__month AS listing__window_start__month
          , subq_0.listing__window_start__quarter AS listing__window_start__quarter
          , subq_0.listing__window_start__year AS listing__window_start__year
          , subq_0.listing__window_start__extract_year AS listing__window_start__extract_year
          , subq_0.listing__window_start__extract_quarter AS listing__window_start__extract_quarter
          , subq_0.listing__window_start__extract_month AS listing__window_start__extract_month
          , subq_0.listing__window_start__extract_day AS listing__window_start__extract_day
          , subq_0.listing__window_start__extract_dow AS listing__window_start__extract_dow
          , subq_0.listing__window_start__extract_doy AS listing__window_start__extract_doy
          , subq_0.listing__window_end__day AS listing__window_end__day
          , subq_0.listing__window_end__week AS listing__window_end__week
          , subq_0.listing__window_end__month AS listing__window_end__month
          , subq_0.listing__window_end__quarter AS listing__window_end__quarter
          , subq_0.listing__window_end__year AS listing__window_end__year
          , subq_0.listing__window_end__extract_year AS listing__window_end__extract_year
          , subq_0.listing__window_end__extract_quarter AS listing__window_end__extract_quarter
          , subq_0.listing__window_end__extract_month AS listing__window_end__extract_month
          , subq_0.listing__window_end__extract_day AS listing__window_end__extract_day
          , subq_0.listing__window_end__extract_dow AS listing__window_end__extract_dow
          , subq_0.listing__window_end__extract_doy AS listing__window_end__extract_doy
          , subq_3.metric_time__month AS metric_time__month
          , subq_0.listing AS listing
          , subq_0.user AS user
          , subq_0.listing__user AS listing__user
          , subq_0.country AS country
          , subq_0.is_lux AS is_lux
          , subq_0.capacity AS capacity
          , subq_0.listing__country AS listing__country
          , subq_0.listing__is_lux AS listing__is_lux
          , subq_0.listing__capacity AS listing__capacity
        FROM (
          -- Read Elements From Semantic Model 'listings'
          SELECT
            listings_src_26000.active_from AS window_start__day
            , DATE_TRUNC('week', listings_src_26000.active_from) AS window_start__week
            , DATE_TRUNC('month', listings_src_26000.active_from) AS window_start__month
            , DATE_TRUNC('quarter', listings_src_26000.active_from) AS window_start__quarter
            , DATE_TRUNC('year', listings_src_26000.active_from) AS window_start__year
            , EXTRACT(year FROM listings_src_26000.active_from) AS window_start__extract_year
            , EXTRACT(quarter FROM listings_src_26000.active_from) AS window_start__extract_quarter
            , EXTRACT(month FROM listings_src_26000.active_from) AS window_start__extract_month
            , EXTRACT(day FROM listings_src_26000.active_from) AS window_start__extract_day
            , EXTRACT(dayofweekiso FROM listings_src_26000.active_from) AS window_start__extract_dow
            , EXTRACT(doy FROM listings_src_26000.active_from) AS window_start__extract_doy
            , listings_src_26000.active_to AS window_end__day
            , DATE_TRUNC('week', listings_src_26000.active_to) AS window_end__week
            , DATE_TRUNC('month', listings_src_26000.active_to) AS window_end__month
            , DATE_TRUNC('quarter', listings_src_26000.active_to) AS window_end__quarter
            , DATE_TRUNC('year', listings_src_26000.active_to) AS window_end__year
            , EXTRACT(year FROM listings_src_26000.active_to) AS window_end__extract_year
            , EXTRACT(quarter FROM listings_src_26000.active_to) AS window_end__extract_quarter
            , EXTRACT(month FROM listings_src_26000.active_to) AS window_end__extract_month
            , EXTRACT(day FROM listings_src_26000.active_to) AS window_end__extract_day
            , EXTRACT(dayofweekiso FROM listings_src_26000.active_to) AS window_end__extract_dow
            , EXTRACT(doy FROM listings_src_26000.active_to) AS window_end__extract_doy
            , listings_src_26000.country
            , listings_src_26000.is_lux
            , listings_src_26000.capacity
            , listings_src_26000.active_from AS listing__window_start__day
            , DATE_TRUNC('week', listings_src_26000.active_from) AS listing__window_start__week
            , DATE_TRUNC('month', listings_src_26000.active_from) AS listing__window_start__month
            , DATE_TRUNC('quarter', listings_src_26000.active_from) AS listing__window_start__quarter
            , DATE_TRUNC('year', listings_src_26000.active_from) AS listing__window_start__year
            , EXTRACT(year FROM listings_src_26000.active_from) AS listing__window_start__extract_year
            , EXTRACT(quarter FROM listings_src_26000.active_from) AS listing__window_start__extract_quarter
            , EXTRACT(month FROM listings_src_26000.active_from) AS listing__window_start__extract_month
            , EXTRACT(day FROM listings_src_26000.active_from) AS listing__window_start__extract_day
            , EXTRACT(dayofweekiso FROM listings_src_26000.active_from) AS listing__window_start__extract_dow
            , EXTRACT(doy FROM listings_src_26000.active_from) AS listing__window_start__extract_doy
            , listings_src_26000.active_to AS listing__window_end__day
            , DATE_TRUNC('week', listings_src_26000.active_to) AS listing__window_end__week
            , DATE_TRUNC('month', listings_src_26000.active_to) AS listing__window_end__month
            , DATE_TRUNC('quarter', listings_src_26000.active_to) AS listing__window_end__quarter
            , DATE_TRUNC('year', listings_src_26000.active_to) AS listing__window_end__year
            , EXTRACT(year FROM listings_src_26000.active_to) AS listing__window_end__extract_year
            , EXTRACT(quarter FROM listings_src_26000.active_to) AS listing__window_end__extract_quarter
            , EXTRACT(month FROM listings_src_26000.active_to) AS listing__window_end__extract_month
            , EXTRACT(day FROM listings_src_26000.active_to) AS listing__window_end__extract_day
            , EXTRACT(dayofweekiso FROM listings_src_26000.active_to) AS listing__window_end__extract_dow
            , EXTRACT(doy FROM listings_src_26000.active_to) AS listing__window_end__extract_doy
            , listings_src_26000.country AS listing__country
            , listings_src_26000.is_lux AS listing__is_lux
            , listings_src_26000.capacity AS listing__capacity
            , listings_src_26000.listing_id AS listing
            , listings_src_26000.user_id AS user
            , listings_src_26000.user_id AS listing__user
          FROM ***************************.dim_listings listings_src_26000
        ) subq_0
        CROSS JOIN (
          -- Pass Only Elements: ['metric_time__month']
          SELECT
            subq_2.metric_time__month
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_1.ds__day
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
              , subq_1.ds__alien_day
              , subq_1.ds__day AS metric_time__day
              , subq_1.ds__week AS metric_time__week
              , subq_1.ds__month AS metric_time__month
              , subq_1.ds__quarter AS metric_time__quarter
              , subq_1.ds__year AS metric_time__year
              , subq_1.ds__extract_year AS metric_time__extract_year
              , subq_1.ds__extract_quarter AS metric_time__extract_quarter
              , subq_1.ds__extract_month AS metric_time__extract_month
              , subq_1.ds__extract_day AS metric_time__extract_day
              , subq_1.ds__extract_dow AS metric_time__extract_dow
              , subq_1.ds__extract_doy AS metric_time__extract_doy
              , subq_1.ds__alien_day AS metric_time__alien_day
            FROM (
              -- Read From Time Spine 'mf_time_spine'
              SELECT
                time_spine_src_26006.ds AS ds__day
                , DATE_TRUNC('week', time_spine_src_26006.ds) AS ds__week
                , DATE_TRUNC('month', time_spine_src_26006.ds) AS ds__month
                , DATE_TRUNC('quarter', time_spine_src_26006.ds) AS ds__quarter
                , DATE_TRUNC('year', time_spine_src_26006.ds) AS ds__year
                , EXTRACT(year FROM time_spine_src_26006.ds) AS ds__extract_year
                , EXTRACT(quarter FROM time_spine_src_26006.ds) AS ds__extract_quarter
                , EXTRACT(month FROM time_spine_src_26006.ds) AS ds__extract_month
                , EXTRACT(day FROM time_spine_src_26006.ds) AS ds__extract_day
                , EXTRACT(dayofweekiso FROM time_spine_src_26006.ds) AS ds__extract_dow
                , EXTRACT(doy FROM time_spine_src_26006.ds) AS ds__extract_doy
                , time_spine_src_26006.alien_day AS ds__alien_day
              FROM ***************************.mf_time_spine time_spine_src_26006
            ) subq_1
          ) subq_2
        ) subq_3
        FULL OUTER JOIN (
          -- Pass Only Elements: ['home_state_latest', 'user']
          SELECT
            subq_4.user
            , subq_4.home_state_latest
          FROM (
            -- Read Elements From Semantic Model 'users_latest'
            SELECT
              DATE_TRUNC('day', users_latest_src_26000.ds) AS ds__day
              , DATE_TRUNC('week', users_latest_src_26000.ds) AS ds__week
              , DATE_TRUNC('month', users_latest_src_26000.ds) AS ds__month
              , DATE_TRUNC('quarter', users_latest_src_26000.ds) AS ds__quarter
              , DATE_TRUNC('year', users_latest_src_26000.ds) AS ds__year
              , EXTRACT(year FROM users_latest_src_26000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM users_latest_src_26000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM users_latest_src_26000.ds) AS ds__extract_month
              , EXTRACT(day FROM users_latest_src_26000.ds) AS ds__extract_day
              , EXTRACT(dayofweekiso FROM users_latest_src_26000.ds) AS ds__extract_dow
              , EXTRACT(doy FROM users_latest_src_26000.ds) AS ds__extract_doy
              , users_latest_src_26000.home_state_latest
              , DATE_TRUNC('day', users_latest_src_26000.ds) AS user__ds__day
              , DATE_TRUNC('week', users_latest_src_26000.ds) AS user__ds__week
              , DATE_TRUNC('month', users_latest_src_26000.ds) AS user__ds__month
              , DATE_TRUNC('quarter', users_latest_src_26000.ds) AS user__ds__quarter
              , DATE_TRUNC('year', users_latest_src_26000.ds) AS user__ds__year
              , EXTRACT(year FROM users_latest_src_26000.ds) AS user__ds__extract_year
              , EXTRACT(quarter FROM users_latest_src_26000.ds) AS user__ds__extract_quarter
              , EXTRACT(month FROM users_latest_src_26000.ds) AS user__ds__extract_month
              , EXTRACT(day FROM users_latest_src_26000.ds) AS user__ds__extract_day
              , EXTRACT(dayofweekiso FROM users_latest_src_26000.ds) AS user__ds__extract_dow
              , EXTRACT(doy FROM users_latest_src_26000.ds) AS user__ds__extract_doy
              , users_latest_src_26000.home_state_latest AS user__home_state_latest
              , users_latest_src_26000.user_id AS user
            FROM ***************************.dim_users_latest users_latest_src_26000
          ) subq_4
        ) subq_5
        ON
          subq_0.user = subq_5.user
      ) subq_6
    ) subq_7
    WHERE user__home_state_latest = 'CA'
  ) subq_8
) subq_9
