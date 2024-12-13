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

-- Create the main ACIT view
--
-- @param ${PROJECT_NAME} The GCP project name
-- @param ${DATASET_NAME} The BQ dataset name

CREATE OR REPLACE VIEW ${PROJECT_NAME}.${DATASET_NAME}.acit AS
WITH
  Products AS (
  SELECT
    P.account_id,
    P.offer_id AS full_id,
    P.product.channel,
    P.product.content_language,
    P.product.feed_label,
    P.product.offer_id,
    P.product.title,
    ARRAY_LENGTH(ARRAY_CONCAT(P.performance_max_campaign_ids, P.shopping_campaign_ids)) AS num_campaigns,
    ARRAY_CONCAT(P.performance_max_campaign_ids, P.shopping_campaign_ids) AS campaign_ids,
    IFNULL(P.product.product_types[SAFE_ORDINAL(1)], '') AS product_type_full,
    ARRAY_LENGTH(SPLIT(IFNULL(P.product.product_types[SAFE_ORDINAL(1)], ''), ' > ')) AS product_type_depth,
    IFNULL(SPLIT(IFNULL(P.product.product_types[SAFE_ORDINAL(1)], ''), ' > ')[SAFE_ORDINAL(1)], '') AS product_type_1,
    IFNULL(SPLIT(IFNULL(P.product.product_types[SAFE_ORDINAL(1)], ''), ' > ')[SAFE_ORDINAL(2)], '') AS product_type_2,
    IFNULL(SPLIT(IFNULL(P.product.product_types[SAFE_ORDINAL(1)], ''), ' > ')[SAFE_ORDINAL(3)], '') AS product_type_3,
    IFNULL(SPLIT(IFNULL(P.product.product_types[SAFE_ORDINAL(1)], ''), ' > ')[SAFE_ORDINAL(4)], '') AS product_type_4,
    IFNULL(SPLIT(IFNULL(P.product.product_types[SAFE_ORDINAL(1)], ''), ' > ')[SAFE_ORDINAL(5)], '') AS product_type_5,
    IFNULL(P.product.custom_label0, '') AS custom_label0,
    IFNULL(P.product.custom_label1, '') AS custom_label1,
    IFNULL(P.product.custom_label2, '') AS custom_label2,
    IFNULL(P.product.custom_label3, '') AS custom_label3,
    IFNULL(P.product.custom_label4, '') AS custom_label4,
    IFNULL(SPLIT(IFNULL(P.product.google_product_category, ''), ' > ')[SAFE_ORDINAL(1)], '') AS product_category_1,
    IFNULL(SPLIT(IFNULL(P.product.google_product_category, ''), ' > ')[SAFE_ORDINAL(2)], '') AS product_category_2,
    IFNULL(SPLIT(IFNULL(P.product.google_product_category, ''), ' > ')[SAFE_ORDINAL(3)], '') AS product_category_3,
    IFNULL(SPLIT(IFNULL(P.product.google_product_category, ''), ' > ')[SAFE_ORDINAL(4)], '') AS product_category_4,
    IFNULL(SPLIT(IFNULL(P.product.google_product_category, ''), ' > ')[SAFE_ORDINAL(5)], '') AS product_category_5,
    P.in_stock,
    ARRAY_LENGTH(ARRAY_CONCAT(P.performance_max_campaign_ids, P.shopping_campaign_ids)) > 0 AS is_targeted,
    ARRAY_LENGTH(P.approved_countries) > 0 AND ARRAY_LENGTH(P.disapproved_countries) = 0 AND ARRAY_LENGTH(P.pending_countries) = 0 AS is_approved,
    IFNULL(P.product.brand, '') AS brand,
    IFNULL(P.product.gtin, '') AS gtin,
  FROM
    `${PROJECT_NAME}.${DATASET_NAME}.products` AS P),
  Ads AS (
  SELECT
    A.segments.productMerchantId,
    A.segments.productChannel,
    A.segments.productLanguage,
    A.segments.productFeedLabel,
    A.segments.productItemId,
    COUNT(DISTINCT A.campaign.id) AS numCampaigns,
    ARRAY_AGG(DISTINCT A.campaign.id) AS campaignIds,
    SUM(A.metrics.impressions) AS impressions,
    SUM(A.metrics.clicks) AS clicks,
    SUM(A.metrics.conversions) AS conversions,
    SUM(A.metrics.costMicros) / 1000000 AS cost,
  FROM
    `${PROJECT_NAME}.${DATASET_NAME}.performance` AS A
  GROUP BY
    1,
    2,
    3,
    4,
    5 )
SELECT
  P.account_id AS merchant_id,
  P.full_id AS offer_id,
  P.channel,
  P.content_language,
  P.feed_label,
  P.offer_id AS product_id,
  P.num_campaigns,
  P.num_campaigns > 0 AS has_campaigns,
  P.campaign_ids,
  P.product_type_depth,
  P.product_type_full,
  P.product_type_1,
  P.product_type_2,
  P.product_type_3,
  P.product_type_4,
  P.product_type_5,
  P.product_category_1,
  P.product_category_2,
  P.product_category_3,
  P.product_category_4,
  P.product_category_5,
  P.custom_label0,
  P.custom_label1,
  P.custom_label2,
  P.custom_label3,
  P.custom_label4,
  IFNULL(A.impressions, 0) AS impressions,
  IFNULL(A.clicks, 0) AS clicks,
  IFNULL(A.conversions, 0.0) AS conversions,
  IFNULL(A.cost, 0.0) AS cost,
  P.in_stock,
  P.is_targeted,
  P.is_approved,
  P.title,
  P.brand,
  P.gtin,
FROM
  `${PROJECT_NAME}.${DATASET_NAME}.language` AS L
INNER JOIN
  Products AS P
ON
  L.languageConstant.code = P.content_language
LEFT JOIN
  Ads AS A
ON
  P.account_id = CAST(A.productMerchantId AS STRING)
  AND LOWER(P.channel) = LOWER(A.productChannel)
  AND L.languageConstant.resourceName = A.productLanguage
  AND LOWER(P.feed_label) = LOWER(A.productFeedLabel)
  AND LOWER(CAST(P.offer_id AS STRING)) = LOWER(CAST(A.productItemId AS STRING));
