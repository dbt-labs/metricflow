-- Aggregate Measures
-- Compute Metrics via Expressions
-- Order By ['user_team']
SELECT
  user_team___team_id
  , user_team___user_id
  , SUM(messages) AS messages
FROM (
  -- Read Elements From Data Source 'messages_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['messages', 'user_team']
  SELECT
    team_id AS user_team___team_id
    , user_id AS user_team___user_id
    , 1 AS messages
  FROM ***************************.fct_messages messages_source_src_10015
) subq_7
GROUP BY
  user_team___team_id
  , user_team___user_id
ORDER BY user_team___team_id DESC, user_team___user_id DESC
