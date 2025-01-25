test_name: test_no_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Pass Only Elements: ['metric_time__day', 'metric_time__martian_day', 'user__bio_added_ts__martian_day', 'user__bio_added_ts__month']
SELECT
  nr_subq_5.user__bio_added_ts__martian_day
  , nr_subq_5.metric_time__martian_day
  , nr_subq_5.user__bio_added_ts__month
  , nr_subq_5.metric_time__day
FROM (
  -- Join Standard Outputs
  -- Join to Custom Granularity Dataset
  -- Join to Custom Granularity Dataset
  SELECT
    nr_subq_0.ds__day AS ds__day
    , nr_subq_0.ds__week AS ds__week
    , nr_subq_0.ds__month AS ds__month
    , nr_subq_0.ds__quarter AS ds__quarter
    , nr_subq_0.ds__year AS ds__year
    , nr_subq_0.ds__extract_year AS ds__extract_year
    , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
    , nr_subq_0.ds__extract_month AS ds__extract_month
    , nr_subq_0.ds__extract_day AS ds__extract_day
    , nr_subq_0.ds__extract_dow AS ds__extract_dow
    , nr_subq_0.ds__extract_doy AS ds__extract_doy
    , nr_subq_0.created_at__day AS created_at__day
    , nr_subq_0.created_at__week AS created_at__week
    , nr_subq_0.created_at__month AS created_at__month
    , nr_subq_0.created_at__quarter AS created_at__quarter
    , nr_subq_0.created_at__year AS created_at__year
    , nr_subq_0.created_at__extract_year AS created_at__extract_year
    , nr_subq_0.created_at__extract_quarter AS created_at__extract_quarter
    , nr_subq_0.created_at__extract_month AS created_at__extract_month
    , nr_subq_0.created_at__extract_day AS created_at__extract_day
    , nr_subq_0.created_at__extract_dow AS created_at__extract_dow
    , nr_subq_0.created_at__extract_doy AS created_at__extract_doy
    , nr_subq_0.ds_partitioned__day AS ds_partitioned__day
    , nr_subq_0.ds_partitioned__week AS ds_partitioned__week
    , nr_subq_0.ds_partitioned__month AS ds_partitioned__month
    , nr_subq_0.ds_partitioned__quarter AS ds_partitioned__quarter
    , nr_subq_0.ds_partitioned__year AS ds_partitioned__year
    , nr_subq_0.ds_partitioned__extract_year AS ds_partitioned__extract_year
    , nr_subq_0.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
    , nr_subq_0.ds_partitioned__extract_month AS ds_partitioned__extract_month
    , nr_subq_0.ds_partitioned__extract_day AS ds_partitioned__extract_day
    , nr_subq_0.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
    , nr_subq_0.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
    , nr_subq_0.last_profile_edit_ts__millisecond AS last_profile_edit_ts__millisecond
    , nr_subq_0.last_profile_edit_ts__second AS last_profile_edit_ts__second
    , nr_subq_0.last_profile_edit_ts__minute AS last_profile_edit_ts__minute
    , nr_subq_0.last_profile_edit_ts__hour AS last_profile_edit_ts__hour
    , nr_subq_0.last_profile_edit_ts__day AS last_profile_edit_ts__day
    , nr_subq_0.last_profile_edit_ts__week AS last_profile_edit_ts__week
    , nr_subq_0.last_profile_edit_ts__month AS last_profile_edit_ts__month
    , nr_subq_0.last_profile_edit_ts__quarter AS last_profile_edit_ts__quarter
    , nr_subq_0.last_profile_edit_ts__year AS last_profile_edit_ts__year
    , nr_subq_0.last_profile_edit_ts__extract_year AS last_profile_edit_ts__extract_year
    , nr_subq_0.last_profile_edit_ts__extract_quarter AS last_profile_edit_ts__extract_quarter
    , nr_subq_0.last_profile_edit_ts__extract_month AS last_profile_edit_ts__extract_month
    , nr_subq_0.last_profile_edit_ts__extract_day AS last_profile_edit_ts__extract_day
    , nr_subq_0.last_profile_edit_ts__extract_dow AS last_profile_edit_ts__extract_dow
    , nr_subq_0.last_profile_edit_ts__extract_doy AS last_profile_edit_ts__extract_doy
    , nr_subq_0.bio_added_ts__second AS bio_added_ts__second
    , nr_subq_0.bio_added_ts__minute AS bio_added_ts__minute
    , nr_subq_0.bio_added_ts__hour AS bio_added_ts__hour
    , nr_subq_0.bio_added_ts__day AS bio_added_ts__day
    , nr_subq_0.bio_added_ts__week AS bio_added_ts__week
    , nr_subq_0.bio_added_ts__month AS bio_added_ts__month
    , nr_subq_0.bio_added_ts__quarter AS bio_added_ts__quarter
    , nr_subq_0.bio_added_ts__year AS bio_added_ts__year
    , nr_subq_0.bio_added_ts__extract_year AS bio_added_ts__extract_year
    , nr_subq_0.bio_added_ts__extract_quarter AS bio_added_ts__extract_quarter
    , nr_subq_0.bio_added_ts__extract_month AS bio_added_ts__extract_month
    , nr_subq_0.bio_added_ts__extract_day AS bio_added_ts__extract_day
    , nr_subq_0.bio_added_ts__extract_dow AS bio_added_ts__extract_dow
    , nr_subq_0.bio_added_ts__extract_doy AS bio_added_ts__extract_doy
    , nr_subq_0.last_login_ts__minute AS last_login_ts__minute
    , nr_subq_0.last_login_ts__hour AS last_login_ts__hour
    , nr_subq_0.last_login_ts__day AS last_login_ts__day
    , nr_subq_0.last_login_ts__week AS last_login_ts__week
    , nr_subq_0.last_login_ts__month AS last_login_ts__month
    , nr_subq_0.last_login_ts__quarter AS last_login_ts__quarter
    , nr_subq_0.last_login_ts__year AS last_login_ts__year
    , nr_subq_0.last_login_ts__extract_year AS last_login_ts__extract_year
    , nr_subq_0.last_login_ts__extract_quarter AS last_login_ts__extract_quarter
    , nr_subq_0.last_login_ts__extract_month AS last_login_ts__extract_month
    , nr_subq_0.last_login_ts__extract_day AS last_login_ts__extract_day
    , nr_subq_0.last_login_ts__extract_dow AS last_login_ts__extract_dow
    , nr_subq_0.last_login_ts__extract_doy AS last_login_ts__extract_doy
    , nr_subq_0.archived_at__hour AS archived_at__hour
    , nr_subq_0.archived_at__day AS archived_at__day
    , nr_subq_0.archived_at__week AS archived_at__week
    , nr_subq_0.archived_at__month AS archived_at__month
    , nr_subq_0.archived_at__quarter AS archived_at__quarter
    , nr_subq_0.archived_at__year AS archived_at__year
    , nr_subq_0.archived_at__extract_year AS archived_at__extract_year
    , nr_subq_0.archived_at__extract_quarter AS archived_at__extract_quarter
    , nr_subq_0.archived_at__extract_month AS archived_at__extract_month
    , nr_subq_0.archived_at__extract_day AS archived_at__extract_day
    , nr_subq_0.archived_at__extract_dow AS archived_at__extract_dow
    , nr_subq_0.archived_at__extract_doy AS archived_at__extract_doy
    , nr_subq_0.user__ds__day AS user__ds__day
    , nr_subq_0.user__ds__week AS user__ds__week
    , nr_subq_0.user__ds__month AS user__ds__month
    , nr_subq_0.user__ds__quarter AS user__ds__quarter
    , nr_subq_0.user__ds__year AS user__ds__year
    , nr_subq_0.user__ds__extract_year AS user__ds__extract_year
    , nr_subq_0.user__ds__extract_quarter AS user__ds__extract_quarter
    , nr_subq_0.user__ds__extract_month AS user__ds__extract_month
    , nr_subq_0.user__ds__extract_day AS user__ds__extract_day
    , nr_subq_0.user__ds__extract_dow AS user__ds__extract_dow
    , nr_subq_0.user__ds__extract_doy AS user__ds__extract_doy
    , nr_subq_0.user__created_at__day AS user__created_at__day
    , nr_subq_0.user__created_at__week AS user__created_at__week
    , nr_subq_0.user__created_at__month AS user__created_at__month
    , nr_subq_0.user__created_at__quarter AS user__created_at__quarter
    , nr_subq_0.user__created_at__year AS user__created_at__year
    , nr_subq_0.user__created_at__extract_year AS user__created_at__extract_year
    , nr_subq_0.user__created_at__extract_quarter AS user__created_at__extract_quarter
    , nr_subq_0.user__created_at__extract_month AS user__created_at__extract_month
    , nr_subq_0.user__created_at__extract_day AS user__created_at__extract_day
    , nr_subq_0.user__created_at__extract_dow AS user__created_at__extract_dow
    , nr_subq_0.user__created_at__extract_doy AS user__created_at__extract_doy
    , nr_subq_0.user__ds_partitioned__day AS user__ds_partitioned__day
    , nr_subq_0.user__ds_partitioned__week AS user__ds_partitioned__week
    , nr_subq_0.user__ds_partitioned__month AS user__ds_partitioned__month
    , nr_subq_0.user__ds_partitioned__quarter AS user__ds_partitioned__quarter
    , nr_subq_0.user__ds_partitioned__year AS user__ds_partitioned__year
    , nr_subq_0.user__ds_partitioned__extract_year AS user__ds_partitioned__extract_year
    , nr_subq_0.user__ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
    , nr_subq_0.user__ds_partitioned__extract_month AS user__ds_partitioned__extract_month
    , nr_subq_0.user__ds_partitioned__extract_day AS user__ds_partitioned__extract_day
    , nr_subq_0.user__ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
    , nr_subq_0.user__ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
    , nr_subq_0.user__last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
    , nr_subq_0.user__last_profile_edit_ts__second AS user__last_profile_edit_ts__second
    , nr_subq_0.user__last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
    , nr_subq_0.user__last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
    , nr_subq_0.user__last_profile_edit_ts__day AS user__last_profile_edit_ts__day
    , nr_subq_0.user__last_profile_edit_ts__week AS user__last_profile_edit_ts__week
    , nr_subq_0.user__last_profile_edit_ts__month AS user__last_profile_edit_ts__month
    , nr_subq_0.user__last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
    , nr_subq_0.user__last_profile_edit_ts__year AS user__last_profile_edit_ts__year
    , nr_subq_0.user__last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
    , nr_subq_0.user__last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
    , nr_subq_0.user__last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
    , nr_subq_0.user__last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
    , nr_subq_0.user__last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
    , nr_subq_0.user__last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
    , nr_subq_0.user__bio_added_ts__second AS user__bio_added_ts__second
    , nr_subq_0.user__bio_added_ts__minute AS user__bio_added_ts__minute
    , nr_subq_0.user__bio_added_ts__hour AS user__bio_added_ts__hour
    , nr_subq_0.user__bio_added_ts__day AS user__bio_added_ts__day
    , nr_subq_0.user__bio_added_ts__week AS user__bio_added_ts__week
    , nr_subq_0.user__bio_added_ts__month AS user__bio_added_ts__month
    , nr_subq_0.user__bio_added_ts__quarter AS user__bio_added_ts__quarter
    , nr_subq_0.user__bio_added_ts__year AS user__bio_added_ts__year
    , nr_subq_0.user__bio_added_ts__extract_year AS user__bio_added_ts__extract_year
    , nr_subq_0.user__bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
    , nr_subq_0.user__bio_added_ts__extract_month AS user__bio_added_ts__extract_month
    , nr_subq_0.user__bio_added_ts__extract_day AS user__bio_added_ts__extract_day
    , nr_subq_0.user__bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
    , nr_subq_0.user__bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
    , nr_subq_0.user__last_login_ts__minute AS user__last_login_ts__minute
    , nr_subq_0.user__last_login_ts__hour AS user__last_login_ts__hour
    , nr_subq_0.user__last_login_ts__day AS user__last_login_ts__day
    , nr_subq_0.user__last_login_ts__week AS user__last_login_ts__week
    , nr_subq_0.user__last_login_ts__month AS user__last_login_ts__month
    , nr_subq_0.user__last_login_ts__quarter AS user__last_login_ts__quarter
    , nr_subq_0.user__last_login_ts__year AS user__last_login_ts__year
    , nr_subq_0.user__last_login_ts__extract_year AS user__last_login_ts__extract_year
    , nr_subq_0.user__last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
    , nr_subq_0.user__last_login_ts__extract_month AS user__last_login_ts__extract_month
    , nr_subq_0.user__last_login_ts__extract_day AS user__last_login_ts__extract_day
    , nr_subq_0.user__last_login_ts__extract_dow AS user__last_login_ts__extract_dow
    , nr_subq_0.user__last_login_ts__extract_doy AS user__last_login_ts__extract_doy
    , nr_subq_0.user__archived_at__hour AS user__archived_at__hour
    , nr_subq_0.user__archived_at__day AS user__archived_at__day
    , nr_subq_0.user__archived_at__week AS user__archived_at__week
    , nr_subq_0.user__archived_at__month AS user__archived_at__month
    , nr_subq_0.user__archived_at__quarter AS user__archived_at__quarter
    , nr_subq_0.user__archived_at__year AS user__archived_at__year
    , nr_subq_0.user__archived_at__extract_year AS user__archived_at__extract_year
    , nr_subq_0.user__archived_at__extract_quarter AS user__archived_at__extract_quarter
    , nr_subq_0.user__archived_at__extract_month AS user__archived_at__extract_month
    , nr_subq_0.user__archived_at__extract_day AS user__archived_at__extract_day
    , nr_subq_0.user__archived_at__extract_dow AS user__archived_at__extract_dow
    , nr_subq_0.user__archived_at__extract_doy AS user__archived_at__extract_doy
    , nr_subq_2.metric_time__day AS metric_time__day
    , nr_subq_0.user AS user
    , nr_subq_0.home_state AS home_state
    , nr_subq_0.user__home_state AS user__home_state
    , nr_subq_0.new_users AS new_users
    , nr_subq_0.archived_users AS archived_users
    , nr_subq_3.martian_day AS metric_time__martian_day
    , nr_subq_4.martian_day AS user__bio_added_ts__martian_day
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS ds__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
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
      , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
      , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
      , users_ds_source_src_28000.user_id AS user
    FROM ***************************.dim_users users_ds_source_src_28000
  ) nr_subq_0
  CROSS JOIN (
    -- Pass Only Elements: ['metric_time__day',]
    SELECT
      nr_subq_1.metric_time__day
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        nr_subq_28019.ds__day
        , nr_subq_28019.ds__week
        , nr_subq_28019.ds__month
        , nr_subq_28019.ds__quarter
        , nr_subq_28019.ds__year
        , nr_subq_28019.ds__extract_year
        , nr_subq_28019.ds__extract_quarter
        , nr_subq_28019.ds__extract_month
        , nr_subq_28019.ds__extract_day
        , nr_subq_28019.ds__extract_dow
        , nr_subq_28019.ds__extract_doy
        , nr_subq_28019.ds__martian_day
        , nr_subq_28019.ds__day AS metric_time__day
        , nr_subq_28019.ds__week AS metric_time__week
        , nr_subq_28019.ds__month AS metric_time__month
        , nr_subq_28019.ds__quarter AS metric_time__quarter
        , nr_subq_28019.ds__year AS metric_time__year
        , nr_subq_28019.ds__extract_year AS metric_time__extract_year
        , nr_subq_28019.ds__extract_quarter AS metric_time__extract_quarter
        , nr_subq_28019.ds__extract_month AS metric_time__extract_month
        , nr_subq_28019.ds__extract_day AS metric_time__extract_day
        , nr_subq_28019.ds__extract_dow AS metric_time__extract_dow
        , nr_subq_28019.ds__extract_doy AS metric_time__extract_doy
        , nr_subq_28019.ds__martian_day AS metric_time__martian_day
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
          , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
          , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
          , time_spine_src_28006.martian_day AS ds__martian_day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) nr_subq_28019
    ) nr_subq_1
  ) nr_subq_2
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_3
  ON
    nr_subq_2.metric_time__day = nr_subq_3.ds
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_4
  ON
    nr_subq_0.user__bio_added_ts__day = nr_subq_4.ds
) nr_subq_5
GROUP BY
  nr_subq_5.user__bio_added_ts__martian_day
  , nr_subq_5.metric_time__martian_day
  , nr_subq_5.user__bio_added_ts__month
  , nr_subq_5.metric_time__day
