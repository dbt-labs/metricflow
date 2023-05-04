-- Join Standard Outputs
-- Pass Only Elements:
--   ['messages', 'user_team__country', 'user_team']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.user_team___team_id AS user_team___team_id
  , subq_10.user_team___user_id AS user_team___user_id
  , users_source_src_10017.country AS user_team__country
  , SUM(subq_10.messages) AS messages
FROM (
  -- Read Elements From Data Source 'messages_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['messages', 'user_team', 'user_team']
  SELECT
    team_id AS user_team___team_id
    , user_id AS user_team___user_id
    , 1 AS messages
  FROM ***************************.fct_messages messages_source_src_10015
) subq_10
LEFT OUTER JOIN
  ***************************.fct_users users_source_src_10017
ON
  (
    subq_10.user_team___team_id = users_source_src_10017.team_id
  ) AND (
    subq_10.user_team___user_id = users_source_src_10017.id
  )
GROUP BY
  subq_10.user_team___team_id
  , subq_10.user_team___user_id
  , users_source_src_10017.country
