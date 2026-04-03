test_name: test_semi_additive_measure_with_where_filter
test_filename: test_query_rendering.py
docstring:
  Tests querying a semi-additive measure with a where filter.
sql_engine: ClickHouse
---
SELECT
  subq_13.user AS user
  , SUM(subq_13.__current_account_balance_by_user) AS current_account_balance_by_user
FROM (
  SELECT
    current_account_balance_by_user AS __current_account_balance_by_user
    , ds__day
    , subq_12.user
  FROM (
    SELECT
      toStartOfDay(ds) AS ds__day
      , user_id AS user
      , account_type AS account__account_type
      , account_balance AS current_account_balance_by_user
    FROM ***************************.fct_accounts accounts_source_src_28000
  ) subq_12
  WHERE account__account_type = 'savings'
) subq_13
INNER JOIN (
  SELECT
    subq_14.user
    , MAX(ds__day) AS ds__day__complete
  FROM (
    SELECT
      ds__day
      , subq_12.user
    FROM (
      SELECT
        toStartOfDay(ds) AS ds__day
        , user_id AS user
        , account_type AS account__account_type
      FROM ***************************.fct_accounts accounts_source_src_28000
    ) subq_12
    WHERE account__account_type = 'savings'
  ) subq_14
  GROUP BY
    subq_14.user
) subq_15
ON
  (
    subq_13.ds__day = subq_15.ds__day__complete
  ) AND (
    subq_13.user = subq_15.user
  )
GROUP BY
  subq_13.user
