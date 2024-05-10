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

CREATE TABLE IF NOT EXISTS ${PROJECT_NAME}.${DATASET_NAME}.MEX_Offer_Funnel_historical
  (
    extraction_date DATE,
    merchant_id STRING,
    merchant_name STRING,
    aggregator_id STRING,
    aggregator_name STRING,
    merchant_name_with_id STRING,
    channel STRING,
    targeted_country STRING,
    product_type_lvl1 STRING,
    product_type_lvl2 STRING,
    product_type_lvl3 STRING,
    custom_label_0 STRING,
    custom_label_1 STRING,
    custom_label_2 STRING,
    custom_label_3 STRING,
    custom_label_4 STRING,
    brand STRING,
    total_offers INT64,
    approved_offers INT64,
    in_stock_offers INT64,
    targeted_offers INT64,
    impression_offers INT64,
    clicked_offers INT64)
  PARTITION BY extraction_date
  OPTIONS (
    partition_expiration_days = 60.0);

CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_Offer_Funnel
  PARTITION BY
    extraction_date
  OPTIONS (
    partition_expiration_days = 90)
AS (
  WITH
    AccountNames AS (
      SELECT DISTINCT
        C.id AS merchant_id,
        C.name AS merchant_name,
        A.settings.id AS aggregator_id,
        A.settings.name AS aggregator_name,
        CONCAT(C.name, ' (', C.id, ')') AS merchant_name_with_id
      FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A, A.children AS C
    ),
    Account AS (
      SELECT DISTINCT
        C.id AS merchant_id,
        A.settings.id AS aggregator_id,
      FROM
        ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A,
        A.children AS C
    ),
    AdsStats AS (
      SELECT
        P.segments.productMerchantId AS merchant_id,
        CONCAT(
          LOWER(P.segments.productChannel),
          ":",
          L.languageConstant.code,
          ":",
          P.segments.productFeedLabel,
          ":",
          P.segments.productItemId) AS product_id,
        SUM(P.metrics.impressions) AS impressions_last30days,
        SUM(P.metrics.clicks) AS clicks_last30days
      FROM ${PROJECT_NAME}.${DATASET_NAME}.performance AS P
      LEFT JOIN ${PROJECT_NAME}.${DATASET_NAME}.language AS L
        ON P.segments.productLanguage = L.languageConstant.resourceName
      GROUP BY
        merchant_id,
        product_id
    ),
    EnabledDestinations AS (
      SELECT DISTINCT
        account_id AS merchant_id,
        offer_id AS product_id,
        EXISTS(
          SELECT 1
          FROM P.status.destination_statuses
          WHERE destination = 'SurfacesAcrossGoogle'
        ) AS has_free_listings_enabled,
        EXISTS(
          SELECT 1
          FROM P.status.destination_statuses
          WHERE destination = 'DisplayAds'
        ) AS has_dynamic_remarketing_enabled,
      FROM
        ${PROJECT_NAME}.${DATASET_NAME}.products AS P,
        P.status.destination_statuses AS DS
    ),
    ProductStatus AS (
      SELECT
        account_id AS merchant_id,
        P.product.channel,
        offer_id AS product_id,
        P.status.item_level_issues,
        ARRAY(
          SELECT DISTINCT x
          FROM
            UNNEST(
              ARRAY_CONCAT(DS.approved_countries, DS.pending_countries, DS.disapproved_countries))
              AS x
        ) AS targeted_countries,
        DS AS destination_statuses,
        ED.has_free_listings_enabled,
        ED.has_dynamic_remarketing_enabled
      FROM ${PROJECT_NAME}.${DATASET_NAME}.products AS P
      LEFT JOIN P.status.destination_statuses AS DS
      INNER JOIN EnabledDestinations AS ED
        ON ED.product_id = P.offer_id
      WHERE DS.destination = 'Shopping'
    ),
    ItemIssues AS (
      SELECT
        P.account_id AS merchant_id,
        P.offer_id AS product_id,
        country,
        ARRAY_AGG(DISTINCT ILI.description) AS item_issues
      FROM
        ${PROJECT_NAME}.${DATASET_NAME}.products AS P,
        P.status.item_level_issues AS ILI,
        ILI.applicable_countries AS country
      WHERE ILI.destination = 'Shopping'
      GROUP BY
        merchant_id,
        product_id,
        country
    ),
    ProductStatusCountry AS (
      SELECT
        CAST(PS.merchant_id AS INT64) AS merchant_id,
        PS.channel,
        PS.product_id,
        targeted_country,
        EXISTS(
          SELECT 1
          FROM PS.destination_statuses.disapproved_countries AS disapproved_country
          WHERE disapproved_country = targeted_country
        ) AS is_disapproved,
        II.item_issues,
        PS.has_free_listings_enabled,
        PS.has_dynamic_remarketing_enabled
      FROM ProductStatus AS PS, PS.targeted_countries AS targeted_country
      LEFT JOIN ItemIssues AS II
        ON
          II.product_id = PS.product_id
          AND II.country = targeted_country
    ),
    Products AS (
      SELECT
        IFNULL(AC.aggregator_id, 0) AS aggregator_id,
        PSC.merchant_id,
        PSC.channel,
        PSC.targeted_country,
        IFNULL(P.product.brand, '') AS brand,
        IFNULL(P.product.custom_label0, '') AS custom_label_0,
        IFNULL(P.product.custom_label1, '') AS custom_label_1,
        IFNULL(P.product.custom_label2, '') AS custom_label_2,
        IFNULL(P.product.custom_label3, '') AS custom_label_3,
        IFNULL(P.product.custom_label4, '') AS custom_label_4,
        IFNULL(SPLIT(P.product.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(0)], '')
          AS product_type_lvl1,
        IFNULL(SPLIT(P.product.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(1)], '')
          AS product_type_lvl2,
        IFNULL(SPLIT(P.product.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(2)], '')
          AS product_type_lvl3,
        PSC.product_id,
        NOT PSC.is_disapproved AS is_approved,
        P.product.availability != 'out of stock' AS is_in_stock,
        (P.has_shopping_targeting OR P.has_performance_max_targeting) AS is_targeted,
        IFNULL(AD.impressions_last30days, 0) > 0 AS had_impressions,
        IFNULL(AD.clicks_last30days, 0) > 0 AS had_clicks,
      FROM ProductStatusCountry AS PSC
      INNER JOIN ${PROJECT_NAME}.${DATASET_NAME}.products AS P
        ON
          CAST(P.account_id AS INT64) = PSC.merchant_id
          AND P.offer_id = PSC.product_id
      LEFT JOIN AdsStats AS AD
        ON
         CAST(AD.merchant_id AS INT64) = CAST(PSC.merchant_id AS INT64)
          AND LOWER(AD.product_id) = LOWER(PSC.product_id)
      LEFT JOIN Account AS AC
        ON AC.merchant_id = PSC.merchant_id
    )
  SELECT
    CURRENT_DATE() AS extraction_date,
    CAST(P.merchant_id AS STRING) AS merchant_id,
    AN.merchant_name,
    CAST(P.aggregator_id AS STRING) AS aggregator_id,
    AN.aggregator_name,
    AN.merchant_name_with_id,
    P.channel,
    P.targeted_country,
    P.product_type_lvl1,
    P.product_type_lvl2,
    P.product_type_lvl3,
    P.custom_label_0,
    P.custom_label_1,
    P.custom_label_2,
    P.custom_label_3,
    P.custom_label_4,
    P.brand,
    COUNT(*) AS total_offers,
    COUNTIF(P.is_approved) AS approved_offers,
    COUNTIF(
      P.is_approved
      AND P.is_in_stock) AS in_stock_offers,
    COUNTIF(
      P.is_approved
      AND P.is_in_stock
      AND P.is_targeted) AS targeted_offers,
    COUNTIF(
      P.is_approved
      AND P.is_in_stock
      AND P.is_targeted
      AND P.had_impressions) AS impression_offers,
    COUNTIF(
      P.is_approved
      AND P.is_in_stock
      AND P.is_targeted
      AND P.had_impressions
      AND P.had_clicks) AS clicked_offers
  FROM Products AS P
  LEFT JOIN AccountNames AS AN
    USING (merchant_id)
  GROUP BY
    extraction_date,
    P.merchant_id,
    AN.merchant_name,
    P.aggregator_id,
    AN.aggregator_name,
    AN.merchant_name_with_id,
    P.channel,
    P.targeted_country,
    P.product_type_lvl1,
    P.product_type_lvl2,
    P.product_type_lvl3,
    P.custom_label_0,
    P.custom_label_1,
    P.custom_label_2,
    P.custom_label_3,
    P.custom_label_4,
    P.brand
);
