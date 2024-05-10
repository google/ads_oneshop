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
