-- Test Approximate Continuous Percentile Expression
SELECT
  approx_percentile(a.col0, 0.5) AS col0_percentile
FROM foo.bar a
