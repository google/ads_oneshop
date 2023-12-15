CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_scores
AS
WITH
  Averages AS (
    SELECT
      REPLACE(D.priority, 'High/Low', 'Low') AS priority,
      AVG(IF(D.comparison_type = 'LESS THAN', 1 - R.benchmark, R.benchmark)) AS avg_benchmark,
      TRUE AS lia_metric
    FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_region AS R
    INNER JOIN ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_details AS D
      ON R.metric_name = D.metric_name
    WHERE R.region_name = 'EMEA'
    GROUP BY 1, 3
    UNION ALL
    SELECT
      REPLACE(D.priority, 'High/Low', 'Low') AS priority,
      AVG(IF(D.comparison_type = 'LESS THAN', 1 - R.benchmark, R.benchmark)) AS avg_benchmark,
      FALSE AS lia_metric
    FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_region AS R
    INNER JOIN ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_details AS D
      ON R.metric_name = D.metric_name
    WHERE
      R.region_name = 'EMEA'
      AND NOT D.lia_metric
    GROUP BY 1, 3
  )
SELECT
  lia_metric,
  (
    (SUM(IF(priority = 'High', avg_benchmark, 0)) * 3)
    + (SUM(IF(priority = 'Medium', avg_benchmark, 0)) * 2)
    + (SUM(IF(priority = 'Low', avg_benchmark, 0))))
    / 6 AS benchmark_score
FROM Averages
GROUP BY 1;
