-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_42.west_coast_balance_first_day) AS FLOAT64) / CAST(NULLIF(MAX(subq_56.east_coast_balance_first_dat), 0) AS FLOAT64) AS regional_starting_balance_ratios
FROM (
  -- Join on MIN(ds) and [] grouping by None
  -- Pass Only Elements: ['total_account_balance_first_day',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(subq_36.total_account_balance_first_day) AS west_coast_balance_first_day
  FROM (
    -- Constrain Output with WHERE
    SELECT
      ds__day
      , total_account_balance_first_day
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
      SELECT
        DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS ds__day
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
        , accounts_source_src_28000.account_balance AS total_account_balance_first_day
      FROM ***************************.fct_accounts accounts_source_src_28000
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        accounts_source_src_28000.user_id = users_latest_src_28000.user_id
    ) subq_35
    WHERE user__home_state_latest IN ('CA', 'HI', 'WA')
  ) subq_36
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Filter row on MIN(ds__day)
    SELECT
      MIN(ds__day) AS ds__day__complete
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
      SELECT
        DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS ds__day
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
      FROM ***************************.fct_accounts accounts_source_src_28000
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        accounts_source_src_28000.user_id = users_latest_src_28000.user_id
    ) subq_35
    WHERE user__home_state_latest IN ('CA', 'HI', 'WA')
  ) subq_38
  ON
    subq_36.ds__day = subq_38.ds__day__complete
) subq_42
CROSS JOIN (
  -- Join on MIN(ds) and [] grouping by None
  -- Pass Only Elements: ['total_account_balance_first_day',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(subq_50.total_account_balance_first_day) AS east_coast_balance_first_dat
  FROM (
    -- Constrain Output with WHERE
    SELECT
      ds__day
      , total_account_balance_first_day
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
      SELECT
        DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS ds__day
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
        , accounts_source_src_28000.account_balance AS total_account_balance_first_day
      FROM ***************************.fct_accounts accounts_source_src_28000
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        accounts_source_src_28000.user_id = users_latest_src_28000.user_id
    ) subq_49
    WHERE user__home_state_latest IN ('MD', 'NY', 'TX')
  ) subq_50
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Filter row on MIN(ds__day)
    SELECT
      MIN(ds__day) AS ds__day__complete
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
      SELECT
        DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS ds__day
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
      FROM ***************************.fct_accounts accounts_source_src_28000
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        accounts_source_src_28000.user_id = users_latest_src_28000.user_id
    ) subq_49
    WHERE user__home_state_latest IN ('MD', 'NY', 'TX')
  ) subq_52
  ON
    subq_50.ds__day = subq_52.ds__day__complete
) subq_56
