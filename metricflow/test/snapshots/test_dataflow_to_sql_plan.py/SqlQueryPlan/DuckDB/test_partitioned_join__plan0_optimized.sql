-- Join Standard Outputs
-- Pass Only Elements:
--   ['identity_verifications', 'user__home_state']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  users_ds_source_src_10007.home_state AS user__home_state
  , SUM(subq_10.identity_verifications) AS identity_verifications
FROM (
  -- Read Elements From Semantic Model 'id_verifications'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['identity_verifications', 'ds_partitioned__day', 'user']
  SELECT
    DATE_TRUNC('day', ds_partitioned) AS ds_partitioned__day
    , user_id AS user
    , 1 AS identity_verifications
  FROM ***************************.fct_id_verifications id_verifications_src_10003
) subq_10
LEFT OUTER JOIN
  ***************************.dim_users users_ds_source_src_10007
ON
  (
    subq_10.user = users_ds_source_src_10007.user_id
  ) AND (
    subq_10.ds_partitioned__day = DATE_TRUNC('day', users_ds_source_src_10007.ds_partitioned)
  )
GROUP BY
  users_ds_source_src_10007.home_state
