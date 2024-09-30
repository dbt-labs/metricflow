-- Compute Metrics via Expressions
SELECT
  subq_5.total_account_balance_first_day_of_month
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_4.total_account_balance_first_day_of_month) AS total_account_balance_first_day_of_month
  FROM (
    -- Join on MIN(ds_month) and [] grouping by None
    -- Pass Only Elements: ['total_account_balance_first_day_of_month',]
    SELECT
      subq_1.total_account_balance_first_day_of_month AS total_account_balance_first_day_of_month
    FROM (
      -- Metric Time Dimension 'ds_month'
      -- Pass Only Elements: ['total_account_balance_first_day_of_month', 'ds_month__month']
      SELECT
        subq_0.ds_month__month
        , subq_0.total_account_balance_first_day_of_month
      FROM (
        -- Read Elements From Semantic Model 'accounts_source'
        SELECT
          accounts_source_src_28000.account_balance
          , accounts_source_src_28000.account_balance AS total_account_balance_first_day
          , accounts_source_src_28000.account_balance AS current_account_balance_by_user
          , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
          , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
          , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
          , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
          , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
          , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
          , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
          , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
          , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
          , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
          , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS ds__extract_dow
          , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
          , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS ds_month__month
          , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS ds_month__quarter
          , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS ds_month__year
          , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS ds_month__extract_year
          , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
          , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS ds_month__extract_month
          , accounts_source_src_28000.account_type
          , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
          , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
          , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
          , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
          , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
          , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
          , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
          , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
          , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
          , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS account__ds__extract_dow
          , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
          , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS account__ds_month__month
          , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS account__ds_month__quarter
          , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS account__ds_month__year
          , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
          , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
          , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
          , accounts_source_src_28000.account_type AS account__account_type
          , accounts_source_src_28000.user_id AS user
          , accounts_source_src_28000.user_id AS account__user
        FROM ***************************.fct_accounts accounts_source_src_28000
      ) subq_0
    ) subq_1
    INNER JOIN (
      -- Filter row on MIN(ds_month__month)
      SELECT
        MIN(subq_2.ds_month__month) AS ds_month__month__complete
      FROM (
        -- Metric Time Dimension 'ds_month'
        -- Pass Only Elements: ['total_account_balance_first_day_of_month', 'ds_month__month']
        SELECT
          subq_0.ds_month__month
          , subq_0.total_account_balance_first_day_of_month
        FROM (
          -- Read Elements From Semantic Model 'accounts_source'
          SELECT
            accounts_source_src_28000.account_balance
            , accounts_source_src_28000.account_balance AS total_account_balance_first_day
            , accounts_source_src_28000.account_balance AS current_account_balance_by_user
            , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
            , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
            , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
            , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
            , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
            , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
            , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS ds__extract_dow
            , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
            , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS ds_month__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS ds_month__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS ds_month__year
            , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS ds_month__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS ds_month__extract_month
            , accounts_source_src_28000.account_type
            , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
            , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
            , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
            , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
            , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
            , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS account__ds__extract_dow
            , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
            , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS account__ds_month__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS account__ds_month__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS account__ds_month__year
            , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
            , accounts_source_src_28000.account_type AS account__account_type
            , accounts_source_src_28000.user_id AS user
            , accounts_source_src_28000.user_id AS account__user
          FROM ***************************.fct_accounts accounts_source_src_28000
        ) subq_0
      ) subq_2
    ) subq_3
    ON
      subq_1.ds_month__month = subq_3.ds_month__month__complete
  ) subq_4
) subq_5
