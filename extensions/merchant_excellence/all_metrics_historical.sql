-- Appends current data to the historical All Metrics table
--
-- @param ${PROJECT_NAME} Name of the project in BigQuery
-- @param ${DATASET_NAME} Name of the dataset within the project in BigQuery

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