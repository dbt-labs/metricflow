-- Join Standard Outputs
-- Pass Only Elements:
--   ['messages', 'user_team__country', 'user_team']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(subq_10.messages) AS messages
  , users_source_src_10016.country AS user_team__country
  , subq_10.user_team___team_id AS user_team___team_id
  , subq_10.user_team___user_id AS user_team___user_id
FROM (
  -- Read Elements From Data Source 'messages_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['messages', 'user_team', 'user_team']
  SELECT
    1 AS messages
    , team_id AS user_team___team_id
    , user_id AS user_team___user_id
  FROM ***************************.fct_messages messages_source_src_10014
) subq_10
LEFT OUTER JOIN
  ***************************.fct_users users_source_src_10016
ON
  (
    subq_10.user_team___team_id = users_source_src_10016.team_id
  ) AND (
    subq_10.user_team___user_id = users_source_src_10016.id
  )
GROUP BY
  users_source_src_10016.country
  , subq_10.user_team___team_id
  , subq_10.user_team___user_id
