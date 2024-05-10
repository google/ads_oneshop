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

SELECT
  extraction_date,
  merchant_id,
  merchant_name,
  merchant_name_with_id,
  aggregator_id,
  aggregator_name,
  channel,
  targeted_country,
  product_type_lvl1,
  product_type_lvl2,
  product_type_lvl3,
  custom_label_0,
  custom_label_1,
  custom_label_2,
  custom_label_3,
  custom_label_4,
  brand,
  metric_name,
  benchmark,
  comparison_type,
  metric_level,
  description,
  support_link,
  metric_category,
  priority,
  lia_metric,
  lia_settings,
  metric_value,
  total_products
FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_All_Metrics;
