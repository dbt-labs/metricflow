-- Aggregate Measures
-- Compute Metrics via Expressions
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
) subq_6
GROUP BY
  user_team___team_id
  , user_team___user_id
