-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(messages) AS messages
  , user_team___team_id
  , user_team___user_id
FROM (
  -- Read Elements From Data Source 'messages_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['messages', 'user_team']
  SELECT
    1 AS messages
    , team_id AS user_team___team_id
    , user_id AS user_team___user_id
  FROM ***************************.fct_messages messages_source_src_10014
) subq_6
GROUP BY
  user_team___team_id
  , user_team___user_id
