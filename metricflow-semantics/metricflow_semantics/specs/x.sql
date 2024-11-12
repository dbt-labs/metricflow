SELECT
  COUNT(DISTINCT bnr_user_count) AS bnr_user_count
  , COUNT(DISTINCT bnr_external_transaction_count) AS bnr_transaction_count
  , SUM(complimentary_item_cost) AS complimentary_item_cost
FROM (
  SELECT
    DATETIME_TRUNC(sem_bnr_transactions_src_10000.transaction_time_est, day) AS metric_time__day
    , sem_user_unit_historical_src_10000.property_id AS user_id__property_id
    , sem_bnr_transactions_src_10000.category AS bnr_transaction_id__category
    , sem_bnr_transactions_src_10000.transaction_id AS bnr_external_transaction_count
    , sem_bnr_transactions_src_10000.user_id AS bnr_user_count
    , sem_bnr_transactions_src_10000.complimentary_item_cost AS complimentary_item_cost
  FROM `bilt-prod`.`analytics`.`stg_all_bnr_transactions` sem_bnr_transactions_src_10000
  LEFT OUTER JOIN
    `bilt-prod`.`analytics`.`stg_user_units_historical` sem_user_unit_historical_src_10000
  ON
    (
      sem_bnr_transactions_src_10000.user_id = sem_user_unit_historical_src_10000.user_id
    ) AND (
      (
        DATETIME_TRUNC(sem_bnr_transactions_src_10000.transaction_time_est, day) >= sem_user_unit_historical_src_10000.user_unit_create_time_est
      ) AND (
        (
          DATETIME_TRUNC(sem_bnr_transactions_src_10000.transaction_time_est, day) < sem_user_unit_historical_src_10000.user_unit_delete_time_est
        ) OR (
          sem_user_unit_historical_src_10000.user_unit_delete_time_est IS NULL
        )
      )
    )
) subq_6
WHERE (bnr_transaction_id__category = 'Fitness' ) AND (user_id__property_id = 378182 and metric_time__day>= '2024-01-01')
LIMIT 100