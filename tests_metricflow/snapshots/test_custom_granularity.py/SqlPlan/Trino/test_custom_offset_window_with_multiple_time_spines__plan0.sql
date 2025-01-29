test_name: test_custom_offset_window_with_multiple_time_spines
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  subq_17.metric_time__hour
  , archived_users AS archived_users_offset_3_martian_days
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_16.metric_time__hour
    , subq_16.archived_users
  FROM (
    -- Aggregate Measures
    SELECT
      subq_15.metric_time__hour
      , SUM(subq_15.archived_users) AS archived_users
    FROM (
      -- Pass Only Elements: ['archived_users', 'metric_time__hour']
      SELECT
        subq_14.metric_time__hour
        , subq_14.archived_users
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_13.metric_time__hour AS metric_time__hour
          , subq_6.ds__day AS ds__day
          , subq_6.ds__week AS ds__week
          , subq_6.ds__month AS ds__month
          , subq_6.ds__quarter AS ds__quarter
          , subq_6.ds__year AS ds__year
          , subq_6.ds__extract_year AS ds__extract_year
          , subq_6.ds__extract_quarter AS ds__extract_quarter
          , subq_6.ds__extract_month AS ds__extract_month
          , subq_6.ds__extract_day AS ds__extract_day
          , subq_6.ds__extract_dow AS ds__extract_dow
          , subq_6.ds__extract_doy AS ds__extract_doy
          , subq_6.created_at__day AS created_at__day
          , subq_6.created_at__week AS created_at__week
          , subq_6.created_at__month AS created_at__month
          , subq_6.created_at__quarter AS created_at__quarter
          , subq_6.created_at__year AS created_at__year
          , subq_6.created_at__extract_year AS created_at__extract_year
          , subq_6.created_at__extract_quarter AS created_at__extract_quarter
          , subq_6.created_at__extract_month AS created_at__extract_month
          , subq_6.created_at__extract_day AS created_at__extract_day
          , subq_6.created_at__extract_dow AS created_at__extract_dow
          , subq_6.created_at__extract_doy AS created_at__extract_doy
          , subq_6.ds_partitioned__day AS ds_partitioned__day
          , subq_6.ds_partitioned__week AS ds_partitioned__week
          , subq_6.ds_partitioned__month AS ds_partitioned__month
          , subq_6.ds_partitioned__quarter AS ds_partitioned__quarter
          , subq_6.ds_partitioned__year AS ds_partitioned__year
          , subq_6.ds_partitioned__extract_year AS ds_partitioned__extract_year
          , subq_6.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
          , subq_6.ds_partitioned__extract_month AS ds_partitioned__extract_month
          , subq_6.ds_partitioned__extract_day AS ds_partitioned__extract_day
          , subq_6.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
          , subq_6.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
          , subq_6.last_profile_edit_ts__millisecond AS last_profile_edit_ts__millisecond
          , subq_6.last_profile_edit_ts__second AS last_profile_edit_ts__second
          , subq_6.last_profile_edit_ts__minute AS last_profile_edit_ts__minute
          , subq_6.last_profile_edit_ts__hour AS last_profile_edit_ts__hour
          , subq_6.last_profile_edit_ts__day AS last_profile_edit_ts__day
          , subq_6.last_profile_edit_ts__week AS last_profile_edit_ts__week
          , subq_6.last_profile_edit_ts__month AS last_profile_edit_ts__month
          , subq_6.last_profile_edit_ts__quarter AS last_profile_edit_ts__quarter
          , subq_6.last_profile_edit_ts__year AS last_profile_edit_ts__year
          , subq_6.last_profile_edit_ts__extract_year AS last_profile_edit_ts__extract_year
          , subq_6.last_profile_edit_ts__extract_quarter AS last_profile_edit_ts__extract_quarter
          , subq_6.last_profile_edit_ts__extract_month AS last_profile_edit_ts__extract_month
          , subq_6.last_profile_edit_ts__extract_day AS last_profile_edit_ts__extract_day
          , subq_6.last_profile_edit_ts__extract_dow AS last_profile_edit_ts__extract_dow
          , subq_6.last_profile_edit_ts__extract_doy AS last_profile_edit_ts__extract_doy
          , subq_6.bio_added_ts__second AS bio_added_ts__second
          , subq_6.bio_added_ts__minute AS bio_added_ts__minute
          , subq_6.bio_added_ts__hour AS bio_added_ts__hour
          , subq_6.bio_added_ts__day AS bio_added_ts__day
          , subq_6.bio_added_ts__week AS bio_added_ts__week
          , subq_6.bio_added_ts__month AS bio_added_ts__month
          , subq_6.bio_added_ts__quarter AS bio_added_ts__quarter
          , subq_6.bio_added_ts__year AS bio_added_ts__year
          , subq_6.bio_added_ts__extract_year AS bio_added_ts__extract_year
          , subq_6.bio_added_ts__extract_quarter AS bio_added_ts__extract_quarter
          , subq_6.bio_added_ts__extract_month AS bio_added_ts__extract_month
          , subq_6.bio_added_ts__extract_day AS bio_added_ts__extract_day
          , subq_6.bio_added_ts__extract_dow AS bio_added_ts__extract_dow
          , subq_6.bio_added_ts__extract_doy AS bio_added_ts__extract_doy
          , subq_6.last_login_ts__minute AS last_login_ts__minute
          , subq_6.last_login_ts__hour AS last_login_ts__hour
          , subq_6.last_login_ts__day AS last_login_ts__day
          , subq_6.last_login_ts__week AS last_login_ts__week
          , subq_6.last_login_ts__month AS last_login_ts__month
          , subq_6.last_login_ts__quarter AS last_login_ts__quarter
          , subq_6.last_login_ts__year AS last_login_ts__year
          , subq_6.last_login_ts__extract_year AS last_login_ts__extract_year
          , subq_6.last_login_ts__extract_quarter AS last_login_ts__extract_quarter
          , subq_6.last_login_ts__extract_month AS last_login_ts__extract_month
          , subq_6.last_login_ts__extract_day AS last_login_ts__extract_day
          , subq_6.last_login_ts__extract_dow AS last_login_ts__extract_dow
          , subq_6.last_login_ts__extract_doy AS last_login_ts__extract_doy
          , subq_6.archived_at__hour AS archived_at__hour
          , subq_6.archived_at__day AS archived_at__day
          , subq_6.archived_at__week AS archived_at__week
          , subq_6.archived_at__month AS archived_at__month
          , subq_6.archived_at__quarter AS archived_at__quarter
          , subq_6.archived_at__year AS archived_at__year
          , subq_6.archived_at__extract_year AS archived_at__extract_year
          , subq_6.archived_at__extract_quarter AS archived_at__extract_quarter
          , subq_6.archived_at__extract_month AS archived_at__extract_month
          , subq_6.archived_at__extract_day AS archived_at__extract_day
          , subq_6.archived_at__extract_dow AS archived_at__extract_dow
          , subq_6.archived_at__extract_doy AS archived_at__extract_doy
          , subq_6.user__ds__day AS user__ds__day
          , subq_6.user__ds__week AS user__ds__week
          , subq_6.user__ds__month AS user__ds__month
          , subq_6.user__ds__quarter AS user__ds__quarter
          , subq_6.user__ds__year AS user__ds__year
          , subq_6.user__ds__extract_year AS user__ds__extract_year
          , subq_6.user__ds__extract_quarter AS user__ds__extract_quarter
          , subq_6.user__ds__extract_month AS user__ds__extract_month
          , subq_6.user__ds__extract_day AS user__ds__extract_day
          , subq_6.user__ds__extract_dow AS user__ds__extract_dow
          , subq_6.user__ds__extract_doy AS user__ds__extract_doy
          , subq_6.user__created_at__day AS user__created_at__day
          , subq_6.user__created_at__week AS user__created_at__week
          , subq_6.user__created_at__month AS user__created_at__month
          , subq_6.user__created_at__quarter AS user__created_at__quarter
          , subq_6.user__created_at__year AS user__created_at__year
          , subq_6.user__created_at__extract_year AS user__created_at__extract_year
          , subq_6.user__created_at__extract_quarter AS user__created_at__extract_quarter
          , subq_6.user__created_at__extract_month AS user__created_at__extract_month
          , subq_6.user__created_at__extract_day AS user__created_at__extract_day
          , subq_6.user__created_at__extract_dow AS user__created_at__extract_dow
          , subq_6.user__created_at__extract_doy AS user__created_at__extract_doy
          , subq_6.user__ds_partitioned__day AS user__ds_partitioned__day
          , subq_6.user__ds_partitioned__week AS user__ds_partitioned__week
          , subq_6.user__ds_partitioned__month AS user__ds_partitioned__month
          , subq_6.user__ds_partitioned__quarter AS user__ds_partitioned__quarter
          , subq_6.user__ds_partitioned__year AS user__ds_partitioned__year
          , subq_6.user__ds_partitioned__extract_year AS user__ds_partitioned__extract_year
          , subq_6.user__ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
          , subq_6.user__ds_partitioned__extract_month AS user__ds_partitioned__extract_month
          , subq_6.user__ds_partitioned__extract_day AS user__ds_partitioned__extract_day
          , subq_6.user__ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
          , subq_6.user__ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
          , subq_6.user__last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
          , subq_6.user__last_profile_edit_ts__second AS user__last_profile_edit_ts__second
          , subq_6.user__last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
          , subq_6.user__last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
          , subq_6.user__last_profile_edit_ts__day AS user__last_profile_edit_ts__day
          , subq_6.user__last_profile_edit_ts__week AS user__last_profile_edit_ts__week
          , subq_6.user__last_profile_edit_ts__month AS user__last_profile_edit_ts__month
          , subq_6.user__last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
          , subq_6.user__last_profile_edit_ts__year AS user__last_profile_edit_ts__year
          , subq_6.user__last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
          , subq_6.user__last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
          , subq_6.user__last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
          , subq_6.user__last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
          , subq_6.user__last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
          , subq_6.user__last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
          , subq_6.user__bio_added_ts__second AS user__bio_added_ts__second
          , subq_6.user__bio_added_ts__minute AS user__bio_added_ts__minute
          , subq_6.user__bio_added_ts__hour AS user__bio_added_ts__hour
          , subq_6.user__bio_added_ts__day AS user__bio_added_ts__day
          , subq_6.user__bio_added_ts__week AS user__bio_added_ts__week
          , subq_6.user__bio_added_ts__month AS user__bio_added_ts__month
          , subq_6.user__bio_added_ts__quarter AS user__bio_added_ts__quarter
          , subq_6.user__bio_added_ts__year AS user__bio_added_ts__year
          , subq_6.user__bio_added_ts__extract_year AS user__bio_added_ts__extract_year
          , subq_6.user__bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
          , subq_6.user__bio_added_ts__extract_month AS user__bio_added_ts__extract_month
          , subq_6.user__bio_added_ts__extract_day AS user__bio_added_ts__extract_day
          , subq_6.user__bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
          , subq_6.user__bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
          , subq_6.user__last_login_ts__minute AS user__last_login_ts__minute
          , subq_6.user__last_login_ts__hour AS user__last_login_ts__hour
          , subq_6.user__last_login_ts__day AS user__last_login_ts__day
          , subq_6.user__last_login_ts__week AS user__last_login_ts__week
          , subq_6.user__last_login_ts__month AS user__last_login_ts__month
          , subq_6.user__last_login_ts__quarter AS user__last_login_ts__quarter
          , subq_6.user__last_login_ts__year AS user__last_login_ts__year
          , subq_6.user__last_login_ts__extract_year AS user__last_login_ts__extract_year
          , subq_6.user__last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
          , subq_6.user__last_login_ts__extract_month AS user__last_login_ts__extract_month
          , subq_6.user__last_login_ts__extract_day AS user__last_login_ts__extract_day
          , subq_6.user__last_login_ts__extract_dow AS user__last_login_ts__extract_dow
          , subq_6.user__last_login_ts__extract_doy AS user__last_login_ts__extract_doy
          , subq_6.user__archived_at__hour AS user__archived_at__hour
          , subq_6.user__archived_at__day AS user__archived_at__day
          , subq_6.user__archived_at__week AS user__archived_at__week
          , subq_6.user__archived_at__month AS user__archived_at__month
          , subq_6.user__archived_at__quarter AS user__archived_at__quarter
          , subq_6.user__archived_at__year AS user__archived_at__year
          , subq_6.user__archived_at__extract_year AS user__archived_at__extract_year
          , subq_6.user__archived_at__extract_quarter AS user__archived_at__extract_quarter
          , subq_6.user__archived_at__extract_month AS user__archived_at__extract_month
          , subq_6.user__archived_at__extract_day AS user__archived_at__extract_day
          , subq_6.user__archived_at__extract_dow AS user__archived_at__extract_dow
          , subq_6.user__archived_at__extract_doy AS user__archived_at__extract_doy
          , subq_6.metric_time__day AS metric_time__day
          , subq_6.metric_time__week AS metric_time__week
          , subq_6.metric_time__month AS metric_time__month
          , subq_6.metric_time__quarter AS metric_time__quarter
          , subq_6.metric_time__year AS metric_time__year
          , subq_6.metric_time__extract_year AS metric_time__extract_year
          , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_6.metric_time__extract_month AS metric_time__extract_month
          , subq_6.metric_time__extract_day AS metric_time__extract_day
          , subq_6.metric_time__extract_dow AS metric_time__extract_dow
          , subq_6.metric_time__extract_doy AS metric_time__extract_doy
          , subq_6.user AS user
          , subq_6.home_state AS home_state
          , subq_6.user__home_state AS user__home_state
          , subq_6.archived_users AS archived_users
        FROM (
          -- Pass Only Elements: ['ts__hour', 'metric_time__hour']
          SELECT
            subq_12.ts__hour
            , subq_12.metric_time__hour
          FROM (
            -- Apply Requested Granularities
            SELECT
              subq_11.ts__hour
              , subq_11.ts__hour__lead AS metric_time__hour
            FROM (
              -- Offset Base Granularity By Custom Granularity Period(s)
              WITH cte_2 AS (
                -- Get Custom Granularity Bounds
                SELECT
                  subq_8.ts__hour AS ts__hour
                  , subq_7.ds__martian_day AS ds__martian_day
                  , FIRST_VALUE(subq_8.ts__hour) OVER (
                    PARTITION BY subq_7.ds__martian_day
                    ORDER BY subq_8.ts__hour
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS ts__hour__first_value
                  , LAST_VALUE(subq_8.ts__hour) OVER (
                    PARTITION BY subq_7.ds__martian_day
                    ORDER BY subq_8.ts__hour
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS ts__hour__last_value
                  , ROW_NUMBER() OVER (
                    PARTITION BY subq_7.ds__martian_day
                    ORDER BY subq_8.ts__hour
                  ) AS ts__hour__row_number
                FROM (
                  -- Read From Time Spine 'mf_time_spine'
                  SELECT
                    time_spine_src_28006.ds AS ds__day
                    , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                    , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                    , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                    , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                    , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                    , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds) AS ds__extract_dow
                    , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                    , time_spine_src_28006.martian_day AS ds__martian_day
                  FROM ***************************.mf_time_spine time_spine_src_28006
                ) subq_7
                INNER JOIN (
                  -- Read From Time Spine 'mf_time_spine_hour'
                  SELECT
                    time_spine_src_28005.ts AS ts__hour
                    , DATE_TRUNC('day', time_spine_src_28005.ts) AS ts__day
                    , DATE_TRUNC('week', time_spine_src_28005.ts) AS ts__week
                    , DATE_TRUNC('month', time_spine_src_28005.ts) AS ts__month
                    , DATE_TRUNC('quarter', time_spine_src_28005.ts) AS ts__quarter
                    , DATE_TRUNC('year', time_spine_src_28005.ts) AS ts__year
                    , EXTRACT(year FROM time_spine_src_28005.ts) AS ts__extract_year
                    , EXTRACT(quarter FROM time_spine_src_28005.ts) AS ts__extract_quarter
                    , EXTRACT(month FROM time_spine_src_28005.ts) AS ts__extract_month
                    , EXTRACT(day FROM time_spine_src_28005.ts) AS ts__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28005.ts) AS ts__extract_dow
                    , EXTRACT(doy FROM time_spine_src_28005.ts) AS ts__extract_doy
                  FROM ***************************.mf_time_spine_hour time_spine_src_28005
                ) subq_8
                ON
                  subq_7.ds__day = subq_8.ts__day
              )

              SELECT
                cte_2.ts__hour AS ts__hour
                , CASE
                  WHEN DATE_ADD('hour', (cte_2.ts__hour__row_number - 1), subq_10.ts__hour__first_value__lead) <= subq_10.ts__hour__last_value__lead
                    THEN DATE_ADD('hour', (cte_2.ts__hour__row_number - 1), subq_10.ts__hour__first_value__lead)
                  ELSE NULL
                END AS ts__hour__lead
              FROM cte_2 cte_2
              INNER JOIN (
                -- Offset Custom Granularity Bounds
                SELECT
                  subq_9.ds__martian_day
                  , LEAD(subq_9.ts__hour__first_value, 3) OVER (ORDER BY subq_9.ds__martian_day) AS ts__hour__first_value__lead
                  , LEAD(subq_9.ts__hour__last_value, 3) OVER (ORDER BY subq_9.ds__martian_day) AS ts__hour__last_value__lead
                FROM (
                  -- Get Unique Rows for Custom Granularity Bounds
                  SELECT
                    cte_2.ds__martian_day
                    , cte_2.ts__hour__first_value
                    , cte_2.ts__hour__last_value
                  FROM cte_2 cte_2
                  GROUP BY
                    cte_2.ds__martian_day
                    , cte_2.ts__hour__first_value
                    , cte_2.ts__hour__last_value
                ) subq_9
              ) subq_10
              ON
                cte_2.ds__martian_day = subq_10.ds__martian_day
            ) subq_11
          ) subq_12
        ) subq_13
        INNER JOIN (
          -- Metric Time Dimension 'archived_at'
          SELECT
            subq_5.ds__day
            , subq_5.ds__week
            , subq_5.ds__month
            , subq_5.ds__quarter
            , subq_5.ds__year
            , subq_5.ds__extract_year
            , subq_5.ds__extract_quarter
            , subq_5.ds__extract_month
            , subq_5.ds__extract_day
            , subq_5.ds__extract_dow
            , subq_5.ds__extract_doy
            , subq_5.created_at__day
            , subq_5.created_at__week
            , subq_5.created_at__month
            , subq_5.created_at__quarter
            , subq_5.created_at__year
            , subq_5.created_at__extract_year
            , subq_5.created_at__extract_quarter
            , subq_5.created_at__extract_month
            , subq_5.created_at__extract_day
            , subq_5.created_at__extract_dow
            , subq_5.created_at__extract_doy
            , subq_5.ds_partitioned__day
            , subq_5.ds_partitioned__week
            , subq_5.ds_partitioned__month
            , subq_5.ds_partitioned__quarter
            , subq_5.ds_partitioned__year
            , subq_5.ds_partitioned__extract_year
            , subq_5.ds_partitioned__extract_quarter
            , subq_5.ds_partitioned__extract_month
            , subq_5.ds_partitioned__extract_day
            , subq_5.ds_partitioned__extract_dow
            , subq_5.ds_partitioned__extract_doy
            , subq_5.last_profile_edit_ts__millisecond
            , subq_5.last_profile_edit_ts__second
            , subq_5.last_profile_edit_ts__minute
            , subq_5.last_profile_edit_ts__hour
            , subq_5.last_profile_edit_ts__day
            , subq_5.last_profile_edit_ts__week
            , subq_5.last_profile_edit_ts__month
            , subq_5.last_profile_edit_ts__quarter
            , subq_5.last_profile_edit_ts__year
            , subq_5.last_profile_edit_ts__extract_year
            , subq_5.last_profile_edit_ts__extract_quarter
            , subq_5.last_profile_edit_ts__extract_month
            , subq_5.last_profile_edit_ts__extract_day
            , subq_5.last_profile_edit_ts__extract_dow
            , subq_5.last_profile_edit_ts__extract_doy
            , subq_5.bio_added_ts__second
            , subq_5.bio_added_ts__minute
            , subq_5.bio_added_ts__hour
            , subq_5.bio_added_ts__day
            , subq_5.bio_added_ts__week
            , subq_5.bio_added_ts__month
            , subq_5.bio_added_ts__quarter
            , subq_5.bio_added_ts__year
            , subq_5.bio_added_ts__extract_year
            , subq_5.bio_added_ts__extract_quarter
            , subq_5.bio_added_ts__extract_month
            , subq_5.bio_added_ts__extract_day
            , subq_5.bio_added_ts__extract_dow
            , subq_5.bio_added_ts__extract_doy
            , subq_5.last_login_ts__minute
            , subq_5.last_login_ts__hour
            , subq_5.last_login_ts__day
            , subq_5.last_login_ts__week
            , subq_5.last_login_ts__month
            , subq_5.last_login_ts__quarter
            , subq_5.last_login_ts__year
            , subq_5.last_login_ts__extract_year
            , subq_5.last_login_ts__extract_quarter
            , subq_5.last_login_ts__extract_month
            , subq_5.last_login_ts__extract_day
            , subq_5.last_login_ts__extract_dow
            , subq_5.last_login_ts__extract_doy
            , subq_5.archived_at__hour
            , subq_5.archived_at__day
            , subq_5.archived_at__week
            , subq_5.archived_at__month
            , subq_5.archived_at__quarter
            , subq_5.archived_at__year
            , subq_5.archived_at__extract_year
            , subq_5.archived_at__extract_quarter
            , subq_5.archived_at__extract_month
            , subq_5.archived_at__extract_day
            , subq_5.archived_at__extract_dow
            , subq_5.archived_at__extract_doy
            , subq_5.user__ds__day
            , subq_5.user__ds__week
            , subq_5.user__ds__month
            , subq_5.user__ds__quarter
            , subq_5.user__ds__year
            , subq_5.user__ds__extract_year
            , subq_5.user__ds__extract_quarter
            , subq_5.user__ds__extract_month
            , subq_5.user__ds__extract_day
            , subq_5.user__ds__extract_dow
            , subq_5.user__ds__extract_doy
            , subq_5.user__created_at__day
            , subq_5.user__created_at__week
            , subq_5.user__created_at__month
            , subq_5.user__created_at__quarter
            , subq_5.user__created_at__year
            , subq_5.user__created_at__extract_year
            , subq_5.user__created_at__extract_quarter
            , subq_5.user__created_at__extract_month
            , subq_5.user__created_at__extract_day
            , subq_5.user__created_at__extract_dow
            , subq_5.user__created_at__extract_doy
            , subq_5.user__ds_partitioned__day
            , subq_5.user__ds_partitioned__week
            , subq_5.user__ds_partitioned__month
            , subq_5.user__ds_partitioned__quarter
            , subq_5.user__ds_partitioned__year
            , subq_5.user__ds_partitioned__extract_year
            , subq_5.user__ds_partitioned__extract_quarter
            , subq_5.user__ds_partitioned__extract_month
            , subq_5.user__ds_partitioned__extract_day
            , subq_5.user__ds_partitioned__extract_dow
            , subq_5.user__ds_partitioned__extract_doy
            , subq_5.user__last_profile_edit_ts__millisecond
            , subq_5.user__last_profile_edit_ts__second
            , subq_5.user__last_profile_edit_ts__minute
            , subq_5.user__last_profile_edit_ts__hour
            , subq_5.user__last_profile_edit_ts__day
            , subq_5.user__last_profile_edit_ts__week
            , subq_5.user__last_profile_edit_ts__month
            , subq_5.user__last_profile_edit_ts__quarter
            , subq_5.user__last_profile_edit_ts__year
            , subq_5.user__last_profile_edit_ts__extract_year
            , subq_5.user__last_profile_edit_ts__extract_quarter
            , subq_5.user__last_profile_edit_ts__extract_month
            , subq_5.user__last_profile_edit_ts__extract_day
            , subq_5.user__last_profile_edit_ts__extract_dow
            , subq_5.user__last_profile_edit_ts__extract_doy
            , subq_5.user__bio_added_ts__second
            , subq_5.user__bio_added_ts__minute
            , subq_5.user__bio_added_ts__hour
            , subq_5.user__bio_added_ts__day
            , subq_5.user__bio_added_ts__week
            , subq_5.user__bio_added_ts__month
            , subq_5.user__bio_added_ts__quarter
            , subq_5.user__bio_added_ts__year
            , subq_5.user__bio_added_ts__extract_year
            , subq_5.user__bio_added_ts__extract_quarter
            , subq_5.user__bio_added_ts__extract_month
            , subq_5.user__bio_added_ts__extract_day
            , subq_5.user__bio_added_ts__extract_dow
            , subq_5.user__bio_added_ts__extract_doy
            , subq_5.user__last_login_ts__minute
            , subq_5.user__last_login_ts__hour
            , subq_5.user__last_login_ts__day
            , subq_5.user__last_login_ts__week
            , subq_5.user__last_login_ts__month
            , subq_5.user__last_login_ts__quarter
            , subq_5.user__last_login_ts__year
            , subq_5.user__last_login_ts__extract_year
            , subq_5.user__last_login_ts__extract_quarter
            , subq_5.user__last_login_ts__extract_month
            , subq_5.user__last_login_ts__extract_day
            , subq_5.user__last_login_ts__extract_dow
            , subq_5.user__last_login_ts__extract_doy
            , subq_5.user__archived_at__hour
            , subq_5.user__archived_at__day
            , subq_5.user__archived_at__week
            , subq_5.user__archived_at__month
            , subq_5.user__archived_at__quarter
            , subq_5.user__archived_at__year
            , subq_5.user__archived_at__extract_year
            , subq_5.user__archived_at__extract_quarter
            , subq_5.user__archived_at__extract_month
            , subq_5.user__archived_at__extract_day
            , subq_5.user__archived_at__extract_dow
            , subq_5.user__archived_at__extract_doy
            , subq_5.archived_at__hour AS metric_time__hour
            , subq_5.archived_at__day AS metric_time__day
            , subq_5.archived_at__week AS metric_time__week
            , subq_5.archived_at__month AS metric_time__month
            , subq_5.archived_at__quarter AS metric_time__quarter
            , subq_5.archived_at__year AS metric_time__year
            , subq_5.archived_at__extract_year AS metric_time__extract_year
            , subq_5.archived_at__extract_quarter AS metric_time__extract_quarter
            , subq_5.archived_at__extract_month AS metric_time__extract_month
            , subq_5.archived_at__extract_day AS metric_time__extract_day
            , subq_5.archived_at__extract_dow AS metric_time__extract_dow
            , subq_5.archived_at__extract_doy AS metric_time__extract_doy
            , subq_5.user
            , subq_5.home_state
            , subq_5.user__home_state
            , subq_5.archived_users
          FROM (
            -- Read Elements From Semantic Model 'users_ds_source'
            SELECT
              1 AS new_users
              , 1 AS archived_users
              , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS ds__day
              , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS ds__week
              , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS ds__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS ds__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS ds__year
              , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds) AS ds__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS ds__extract_doy
              , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS created_at__day
              , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS created_at__week
              , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS created_at__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS created_at__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS created_at__year
              , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS created_at__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS created_at__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS created_at__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS created_at__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS created_at__extract_doy
              , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
              , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__week
              , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
              , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
              , users_ds_source_src_28000.home_state
              , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
              , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
              , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
              , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
              , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__week
              , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
              , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
              , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
              , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
              , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
              , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__week
              , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
              , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
              , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
              , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
              , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS last_login_ts__week
              , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
              , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
              , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS archived_at__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS archived_at__day
              , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS archived_at__week
              , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS archived_at__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS archived_at__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS archived_at__year
              , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
              , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS user__ds__day
              , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS user__ds__week
              , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS user__ds__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS user__ds__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS user__ds__year
              , EXTRACT(year FROM users_ds_source_src_28000.ds) AS user__ds__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS user__ds__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.ds) AS user__ds__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.ds) AS user__ds__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS user__ds__extract_doy
              , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS user__created_at__day
              , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS user__created_at__week
              , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS user__created_at__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS user__created_at__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS user__created_at__year
              , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
              , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
              , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__week
              , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
              , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
              , users_ds_source_src_28000.home_state AS user__home_state
              , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
              , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
              , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
              , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
              , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__week
              , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
              , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
              , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
              , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
              , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
              , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__week
              , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
              , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
              , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
              , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
              , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__week
              , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
              , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
              , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS user__archived_at__hour
              , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS user__archived_at__day
              , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS user__archived_at__week
              , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS user__archived_at__month
              , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
              , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS user__archived_at__year
              , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
              , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
              , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
              , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
              , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
              , users_ds_source_src_28000.user_id AS user
            FROM ***************************.dim_users users_ds_source_src_28000
          ) subq_5
        ) subq_6
        ON
          subq_13.ts__hour = subq_6.metric_time__hour
      ) subq_14
    ) subq_15
    GROUP BY
      subq_15.metric_time__hour
  ) subq_16
) subq_17
