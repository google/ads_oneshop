-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_scores
AS (
  WITH
    Averages AS (
      SELECT
        REPLACE(D.priority, 'High/Low', 'Low') AS priority,
        TRUE AS lia_metric,
        AVG(IF(D.comparison_type = 'LESS THAN', 1 - V.benchmark, V.benchmark)) AS avg_benchmark
      FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_values AS V
      INNER JOIN ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_details AS D
        ON V.metric_name = D.metric_name
      GROUP BY 1, 2
      UNION ALL
      SELECT
        REPLACE(D.priority, 'High/Low', 'Low') AS priority,
        FALSE AS lia_metric,
        AVG(IF(D.comparison_type = 'LESS THAN', 1 - V.benchmark, V.benchmark)) AS avg_benchmark
      FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_values AS V
      INNER JOIN ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_details AS D
        ON V.metric_name = D.metric_name
      WHERE
        NOT D.lia_metric
      GROUP BY 1, 2
    )
  SELECT
    lia_metric,
    IF(lia_metric, 'Uncheck this one to remove LIA metrics', 'Check both to include LIA metrics')
      AS lia_settings,
    (
      (SUM(IF(priority = 'High', avg_benchmark, 0)) * 3)
      + (SUM(IF(priority = 'Medium', avg_benchmark, 0)) * 2)
      + (SUM(IF(priority = 'Low', avg_benchmark, 0))))
      / 6 AS benchmark_score
  FROM Averages
  GROUP BY 1, 2
);
