-- Join Standard Outputs
-- Pass Only Elements:
--   ['identity_verifications', 'user__home_state']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  users_ds_source_src_10006.home_state AS user__home_state
  , SUM(subq_10.identity_verifications) AS identity_verifications
FROM (
  -- Read Elements From Data Source 'id_verifications'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['identity_verifications', 'user', 'ds_partitioned']
  SELECT
    ds_partitioned
    , user_id AS user
    , 1 AS identity_verifications
  FROM ***************************.fct_id_verifications id_verifications_src_10002
) subq_10
LEFT OUTER JOIN
  ***************************.dim_users users_ds_source_src_10006
ON
  (
    subq_10.user = users_ds_source_src_10006.user_id
  ) AND (
    subq_10.ds_partitioned = users_ds_source_src_10006.ds_partitioned
  )
GROUP BY
  user__home_state
