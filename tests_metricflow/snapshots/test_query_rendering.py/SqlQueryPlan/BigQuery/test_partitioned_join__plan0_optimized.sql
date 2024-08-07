-- Join Standard Outputs
-- Pass Only Elements: ['identity_verifications', 'user__home_state']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  users_ds_source_src_28000.home_state AS user__home_state
  , SUM(subq_11.identity_verifications) AS identity_verifications
FROM (
  -- Read Elements From Semantic Model 'id_verifications'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['identity_verifications', 'ds_partitioned__day', 'user']
  SELECT
    DATETIME_TRUNC(ds_partitioned, day) AS ds_partitioned__day
    , user_id AS user
    , 1 AS identity_verifications
  FROM ***************************.fct_id_verifications id_verifications_src_28000
) subq_11
LEFT OUTER JOIN
  ***************************.dim_users users_ds_source_src_28000
ON
  (
    subq_11.user = users_ds_source_src_28000.user_id
  ) AND (
    subq_11.ds_partitioned__day = DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, day)
  )
GROUP BY
  user__home_state
