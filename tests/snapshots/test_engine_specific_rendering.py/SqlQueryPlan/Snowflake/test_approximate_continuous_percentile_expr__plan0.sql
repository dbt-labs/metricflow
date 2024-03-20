-- Test Approximate Continuous Percentile Expression
SELECT
  APPROX_PERCENTILE(a.col0, 0.5) AS col0_percentile
FROM foo.bar a
