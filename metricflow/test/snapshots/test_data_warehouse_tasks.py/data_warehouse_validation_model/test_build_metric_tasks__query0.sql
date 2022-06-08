-- Read Elements From Data Source 'animals'
-- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
-- Pass Only Elements:
--   ['count_dogs', 'ds']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(is_dog) AS count_dogs
  , ds
FROM (
  SELECT true AS is_dog, '2022-06-01' AS ds
) animals_src_0
WHERE (
  ds >= CAST('2000-01-01' AS TEXT)
) AND (
  ds <= CAST('2040-12-31' AS TEXT)
)
GROUP BY
  ds