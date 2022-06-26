-- Join Standard Outputs
-- Pass Only Elements:
--   ['identity_verifications', 'user__home_state']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(subq_10.identity_verifications) AS identity_verifications
  , users_ds_source_src_10006.home_state AS user__home_state
FROM (
  -- Read Elements From Data Source 'id_verifications'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['identity_verifications', 'user', 'ds_partitioned']
  SELECT
    1 AS identity_verifications
    , ds_partitioned
    , user_id AS user
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
  users_ds_source_src_10006.home_state
