-- Appends current data to the historical Offer Funnel table
--
-- @param ${PROJECT_NAME} Name of the project in BigQuery
-- @param ${DATASET_NAME} Name of the dataset within the project in BigQuery

SELECT
  extraction_date,
  merchant_id,
  merchant_name,
  aggregator_id,
  aggregator_name,
  merchant_name_with_id,
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
  total_offers,
  approved_offers,
  in_stock_offers,
  targeted_offers,
  impression_offers,
  clicked_offers
FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_Offer_Funnel;