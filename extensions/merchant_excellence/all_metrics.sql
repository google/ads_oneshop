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

CREATE TABLE IF NOT EXISTS ${PROJECT_NAME}.${DATASET_NAME}.MEX_All_Metrics_historical
  (
    extraction_date DATE,
    merchant_id STRING,
    merchant_name STRING,
    merchant_name_with_id STRING,
    aggregator_id STRING,
    aggregator_name STRING,
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
    metric_name STRING,
    benchmark FLOAT64,
    comparison_type STRING,
    metric_level STRING,
    description STRING,
    support_link STRING,
    metric_category STRING,
    priority STRING,
    lia_metric BOOL,
    lia_settings STRING,
    metric_value INT64,
    total_products INT64)
  PARTITION BY extraction_date
  OPTIONS (
    partition_expiration_days = 60.0);

CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
  OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE))
AS
WITH
  -- Native Merchant API v1 omnichannel settings. The `liasettings` table
  -- is now FLAT -- one row per account with a repeated per-region
  -- `omnichannel_settings` list (the old {settings, children[]} envelope is gone;
  -- v1 lists settings per sub-/standalone account directly). Status strings became
  -- enum NAME strings ('active' -> 'ACTIVE'); hostedLocalStorefront + mHLSF folded
  -- into the single `lsf_type` enum.
  Lia AS (
    SELECT DISTINCT
      L.account_id AS merchant_id,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE
          in_stock.state = 'ACTIVE'
          AND inventory_verification.contact_state = 'ACTIVE'
          AND about.state = 'ACTIVE'
      ) AS lia_has_lia_implemented,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE lsf_type IN ('GHLSF', 'MHLSF_BASIC', 'MHLSF_FULL')
      ) AS lia_has_mhlsf_implemented,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE pickup.state = 'ACTIVE'
      ) AS lia_has_store_pickup_implemented,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE odo.state = 'ACTIVE'
      ) AS lia_has_odo_implemented
    FROM ${PROJECT_NAME}.${DATASET_NAME}.liasettings AS L
  ),
  Programs AS (
    SELECT
      account_id AS merchant_id,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('local-inventory-ads', 'local_inventory_ads')
        AND prog.state IN ('ENABLED', 'ELIGIBLE')
      ) AS lia_is_lia_ready_or_started,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('local-inventory-ads', 'local_inventory_ads')
        AND prog.state = 'ENABLED'
      ) AS lia_is_lia_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('promotions')
        AND prog.state = 'ENABLED'
      ) AS has_merchant_promotions_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('product-reviews', 'product_reviews')
        AND prog.state = 'ENABLED'
      ) AS has_product_reviews_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('checkout', 'shopping-actions', 'shopping_actions')
        AND prog.state = 'ENABLED'
      ) AS has_checkout_on_merchant_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('checkout', 'shopping-actions', 'shopping_actions')
        AND (ARRAY_LENGTH(IFNULL(JSON_QUERY_ARRAY(TO_JSON(prog), '$.unmet_requirements'), [])) = 0 OR prog.state = 'ENABLED')
      ) AS has_cwcd_implemented
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A
    LEFT JOIN UNNEST(A.programs) AS prog
    GROUP BY 1
  ),
  ActiveReturns AS (
    SELECT DISTINCT
      account_id AS merchant_id,
      ARRAY_LENGTH(R.returns) > 0 AS has_returns_enabled
    FROM ${PROJECT_NAME}.${DATASET_NAME}.returns AS R
  ),
  ActivePromotionsCount AS (
    SELECT
      account_id AS merchant_id,
      COUNTIF(
        COALESCE(
          SAFE.DATE(
            P.attributes.promotion_effective_time_period.end_time
          ),
          SAFE.DATE(
            P.attributes.promotion_effective_time_period.start_time
          ),
          SAFE.DATE(
            P.attributes.promotion_display_time_period.end_time
          ),
          SAFE.DATE(
            P.attributes.promotion_display_time_period.start_time
          )
        ) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
      ) AS active_promotions_count
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.promotions AS Pr,
      UNNEST(Pr.promotions) AS P
    GROUP BY 1
  ),
  Reports AS (
    SELECT
      account_id AS merchant_id,
      LOGICAL_OR(IFNULL(has_market_insights, FALSE)) AS has_market_insights,
      LOGICAL_OR(
        IFNULL(ARRAY_LENGTH(structured_data_issues) > 0, FALSE)
      ) AS uses_structured_data
    FROM ${PROJECT_NAME}.${DATASET_NAME}.reports
    GROUP BY 1
  ),
  CurbsidePickup AS (
    SELECT
      CAST(account_id AS INT64) AS merchant_id,
      LOGICAL_OR(
        UPPER(COALESCE(channel, '')) = 'LOCAL'
        AND JSON_VALUE(TO_JSON(product.product_attributes), '$.pickup_method') = 'curbside'
      ) AS lia_has_curbside_pickup_implemented
    FROM ${PROJECT_NAME}.${DATASET_NAME}.products
    GROUP BY 1
  ),
  AllShippingData AS (
    -- Native Merchant API v1 flat per-account shipping settings. The
    -- old {settings, children[]} envelope is gone; each row is one account.
    SELECT
      account_id AS accountId,
      services
    FROM ${PROJECT_NAME}.${DATASET_NAME}.shippingsettings
  ),
  AccountLevelShipping AS (
    SELECT DISTINCT
      accountId AS merchant_id,
      ARRAY_LENGTH(services) > 0 AS has_account_level_shipping,
      EXISTS(
        SELECT *
        FROM SS.services
        WHERE
          delivery_time.max_transit_days IS NOT NULL
          AND delivery_time.min_transit_days IS NOT NULL
          AND delivery_time.min_handling_days IS NOT NULL
          AND delivery_time.max_handling_days IS NOT NULL
      ) AS has_account_level_shipping_speed,
      EXISTS(
        SELECT *
        FROM SS.services
        WHERE
          delivery_time.max_transit_days IS NOT NULL
          AND delivery_time.max_handling_days IS NOT NULL
          AND delivery_time.max_transit_days + delivery_time.max_handling_days <= 3
      ) AS has_account_level_fast_shipping,
      EXISTS(
        SELECT *
        FROM
          SS.services AS S,
          S.rate_groups AS RG,
          RG.main_table.rows AS RS,
          RS.cells AS C
        WHERE
          C.flat_rate.amount_micros = 0
      )
        OR EXISTS(
          SELECT *
          FROM
            SS.services AS S,
            S.rate_groups AS RG
          WHERE RG.single_value.flat_rate.amount_micros = 0
        ) AS has_account_level_free_shipping
    FROM AllShippingData AS SS
  ),
  EnabledDestinations AS (
    SELECT DISTINCT
      account_id AS merchant_id,
      offer_id AS product_id,
      EXISTS(
        SELECT 1
        FROM P.status.destination_statuses
        WHERE reporting_context = 'FREE_LISTINGS'
      ) AS has_free_listings_enabled,
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.products AS P,
      P.status.destination_statuses AS DS
  ),
  AllAccounts AS (
    -- Flat Merchant API v1 accounts: one row per (leaf) account. Advanced/MCA
    -- accounts are excluded as merchants (is_advanced); the parent account is
    -- self-joined for the aggregator name and to roll down account-level
    -- automatic-improvements settings to sub-accounts.
    SELECT
      A.account_id AS merchant_id,
      A.account_name AS merchant_name,
      IFNULL(A.parent_account, 0) AS aggregator_id,
      P.account_name AS aggregator_name,
      COALESCE(
        A.automatic_improvements.image_improvements.effective_allow_automatic_image_improvements,
        P.automatic_improvements.image_improvements.effective_allow_automatic_image_improvements,
        FALSE)
        AS has_image_aiu_enabled,
      COALESCE(
        (
          A.automatic_improvements.item_updates.effective_allow_strict_availability_updates
          OR A.automatic_improvements.item_updates.effective_allow_availability_updates),
        (
          P.automatic_improvements.item_updates.effective_allow_strict_availability_updates
          OR P.automatic_improvements.item_updates.effective_allow_availability_updates),
        FALSE)
        AS has_availability_aiu_enabled,
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A
    LEFT JOIN ${PROJECT_NAME}.${DATASET_NAME}.accounts AS P
      ON A.parent_account = P.account_id
    WHERE NOT A.is_advanced
  ),
  AccountNames AS (
    SELECT DISTINCT
      A.merchant_id,
      A.merchant_name,
      A.aggregator_id,
      A.aggregator_name,
      CONCAT(A.merchant_name, ' (', A.merchant_id, ')') AS merchant_name_with_id
    FROM AllAccounts AS A
  ),
  ProductFeatures AS (
    SELECT
      CAST(account_id AS INT64) AS merchant_id,
      LOGICAL_OR(IFNULL(JSON_VALUE(TO_JSON(product.product_attributes), '$.ads_redirect'), '') != '') AS has_ads_redirect,
      LOGICAL_OR(IFNULL(JSON_VALUE(TO_JSON(product.product_attributes), '$.video_link'), '') != '') AS has_valid_video_link,
      LOGICAL_OR(ARRAY_LENGTH(JSON_QUERY_ARRAY(TO_JSON(product.product_attributes), '$.loyalty_programs')) > 0) AS has_loyalty_member_pricing_activated
    FROM ${PROJECT_NAME}.${DATASET_NAME}.products
    GROUP BY 1
  ),
  Account AS (
    SELECT DISTINCT
      A.merchant_id,
      A.aggregator_id,
      IFNULL(A.has_image_aiu_enabled, FALSE) AS has_image_aiu_enabled,
      IFNULL(A.has_availability_aiu_enabled, FALSE) AS has_availability_aiu_enabled,
      IFNULL(L.lia_has_lia_implemented, FALSE) AS lia_has_lia_implemented,
      IFNULL(L.lia_has_mhlsf_implemented, FALSE) AS lia_has_mhlsf_implemented,
      IFNULL(L.lia_has_store_pickup_implemented, FALSE) AS lia_has_store_pickup_implemented,
      IFNULL(L.lia_has_odo_implemented, FALSE) AS lia_has_odo_implemented,
      IFNULL(ALS.has_account_level_shipping, FALSE) AS has_account_level_shipping,
      IFNULL(ALS.has_account_level_shipping_speed, FALSE) AS has_account_level_shipping_speed,
      IFNULL(ALS.has_account_level_fast_shipping, FALSE) AS has_account_level_fast_shipping,
      IFNULL(ALS.has_account_level_free_shipping, FALSE) AS has_account_level_free_shipping,
      IFNULL(PF.has_ads_redirect, FALSE) AS has_ads_redirect,
      IFNULL(PF.has_valid_video_link, FALSE) AS has_valid_video_link,
      IFNULL(PF.has_loyalty_member_pricing_activated, FALSE) AS has_loyalty_member_pricing_activated,
      IFNULL(Prog.lia_is_lia_ready_or_started, FALSE) AS lia_is_lia_ready_or_started,
      IFNULL(Prog.lia_is_lia_enabled, FALSE) AS lia_is_lia_enabled,
      IFNULL(Prog.has_merchant_promotions_enabled, FALSE) AS has_merchant_promotions_enabled,
      IFNULL(Prog.has_product_reviews_enabled, FALSE) AS has_product_reviews_enabled,
      IFNULL(Prog.has_checkout_on_merchant_enabled, FALSE) AS has_checkout_on_merchant_enabled,
      IFNULL(Prog.has_cwcd_implemented, FALSE) AS has_cwcd_implemented,
      IFNULL(AR.has_returns_enabled, FALSE) AS has_returns_enabled,
      IFNULL(APC.active_promotions_count, 0) AS active_promotions_count,
      IFNULL(Rep.has_market_insights, FALSE) AS has_market_insights,
      IFNULL(Rep.uses_structured_data, FALSE) AS uses_structured_data,
      IFNULL(CP.lia_has_curbside_pickup_implemented, FALSE) AS lia_has_curbside_pickup_implemented,
    FROM
      AllAccounts AS A
    LEFT JOIN Lia AS L
      ON L.merchant_id = A.merchant_id
    LEFT JOIN AccountLevelShipping AS ALS
      ON ALS.merchant_id = A.merchant_id
    LEFT JOIN ProductFeatures AS PF
      ON PF.merchant_id = A.merchant_id
    LEFT JOIN Programs AS Prog
      ON Prog.merchant_id = A.merchant_id
    LEFT JOIN ActiveReturns AS AR
      ON AR.merchant_id = A.merchant_id
    LEFT JOIN ActivePromotionsCount AS APC
      ON APC.merchant_id = A.merchant_id
    LEFT JOIN Reports AS Rep
      ON Rep.merchant_id = A.merchant_id
    LEFT JOIN CurbsidePickup AS CP
      ON CP.merchant_id = A.merchant_id
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
  ProductStatus AS (
    SELECT
      account_id AS merchant_id,
      COALESCE(P.channel, 'online') AS channel,
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
      ED.has_free_listings_enabled
    FROM ${PROJECT_NAME}.${DATASET_NAME}.products AS P
    LEFT JOIN P.status.destination_statuses AS DS
    LEFT JOIN EnabledDestinations AS ED
      ON ED.product_id = P.offer_id
      AND ED.merchant_id = P.account_id
    WHERE DS.reporting_context = 'SHOPPING_ADS'
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
    WHERE ILI.reporting_context = 'SHOPPING_ADS'
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
      PS.has_free_listings_enabled
    FROM ProductStatus AS PS, PS.targeted_countries AS targeted_country
    LEFT JOIN ItemIssues AS II
      ON
        II.product_id = PS.product_id
        AND II.country = targeted_country
        AND II.merchant_id = PS.merchant_id
  )
SELECT
  IFNULL(AC.aggregator_id, 0) AS aggregator_id,
  PSC.merchant_id,
  PSC.channel,
  PSC.product_id,
  PSC.targeted_country,
  PSC.is_disapproved,
  PSC.item_issues,
  PSC.has_free_listings_enabled,
  P.product.offer_id AS item_id,
  IFNULL(P.product.product_attributes.brand, '') AS brand,
  IFNULL(P.product.product_attributes.custom_label_0, '') AS custom_label_0,
  IFNULL(P.product.product_attributes.custom_label_1, '') AS custom_label_1,
  IFNULL(P.product.product_attributes.custom_label_2, '') AS custom_label_2,
  IFNULL(P.product.product_attributes.custom_label_3, '') AS custom_label_3,
  IFNULL(P.product.product_attributes.custom_label_4, '') AS custom_label_4,
  IFNULL(SPLIT(P.product.product_attributes.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(0)], '')
    AS product_type_lvl1,
  IFNULL(SPLIT(P.product.product_attributes.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(1)], '')
    AS product_type_lvl2,
  IFNULL(SPLIT(P.product.product_attributes.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(2)], '')
    AS product_type_lvl3,
  P.product.product_attributes.gtins[SAFE_ORDINAL(1)] AS gtin,
  P.product.product_attributes.description,
  P.product.product_attributes.title,
  P.product.product_attributes.color,
  P.product.product_attributes.age_group,
  P.product.product_attributes.gender,
  P.product.product_attributes.size,
  P.product.product_attributes.additional_image_links,
  P.product.product_attributes.lifestyle_image_links,
  P.product.product_attributes.sale_price,
  P.product.product_attributes.item_group_id,
  P.product.product_attributes.product_types,
  P.product.product_attributes.product_highlights,
  P.product.product_attributes.shipping,
  P.product.product_attributes.cost_of_goods_sold,
  P.product.product_attributes.condition,
  JSON_QUERY(TO_JSON(P.product.product_attributes), '$.installment') AS installment,
  JSON_VALUE(TO_JSON(P.product.product_attributes), '$.ads_redirect') AS ads_redirect,
  JSON_VALUE(TO_JSON(P.product.product_attributes), '$.video_link') AS video_link,
  JSON_QUERY_ARRAY(TO_JSON(P.product.product_attributes), '$.loyalty_programs') AS loyalty_programs,
  P.status.destination_statuses AS destination_statuses,
  (P.has_shopping_targeting OR P.has_performance_max_targeting) AS has_targeting,
  IFNULL(AD.impressions_last30days, 0) > 0 AS had_impressions,
  IFNULL(AD.clicks_last30days, 0) > 0 AS had_clicks,
  AC.has_image_aiu_enabled,
  AC.has_availability_aiu_enabled,
  AC.lia_has_lia_implemented,
  AC.lia_has_mhlsf_implemented,
  AC.lia_has_store_pickup_implemented,
  AC.lia_has_odo_implemented,
  AC.has_account_level_shipping,
  AC.has_account_level_shipping_speed,
  AC.has_account_level_fast_shipping,
  AC.has_account_level_free_shipping,
  AC.has_ads_redirect,
  AC.has_valid_video_link,
  AC.has_loyalty_member_pricing_activated,
  AC.lia_is_lia_ready_or_started,
  AC.lia_is_lia_enabled,
  AC.has_merchant_promotions_enabled,
  AC.has_product_reviews_enabled,
  AC.has_checkout_on_merchant_enabled,
  AC.has_cwcd_implemented,
  AC.has_returns_enabled,
  AC.active_promotions_count,
  AC.has_market_insights,
  AC.uses_structured_data,
  AC.lia_has_curbside_pickup_implemented
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
  ON AC.merchant_id = PSC.merchant_id;

CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_All_Metrics
  PARTITION BY
    extraction_date
  OPTIONS (
    partition_expiration_days = 90)
AS
WITH
  Benchmarks AS (
    SELECT
      BV.metric_name,
      BV.benchmark,
      BD.* EXCEPT (metric_name)
    FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_values AS BV
    INNER JOIN
      ${PROJECT_NAME}.${DATASET_NAME}.MEX_benchmark_details AS BD
      ON BD.metric_name = BV.metric_name
  ),
  AllAccounts AS (
    -- Flat Merchant API v1 accounts: one row per (leaf) account; parent
    -- self-joined for the aggregator name. Advanced/MCA accounts are excluded.
    SELECT
      A.account_id AS merchant_id,
      A.account_name AS merchant_name,
      IFNULL(A.parent_account, 0) AS aggregator_id,
      P.account_name AS aggregator_name,
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A
    LEFT JOIN ${PROJECT_NAME}.${DATASET_NAME}.accounts AS P
      ON A.parent_account = P.account_id
    WHERE NOT A.is_advanced
  ),
  AccountNames AS (
    SELECT DISTINCT
      A.merchant_id,
      A.merchant_name,
      A.aggregator_id,
      A.aggregator_name,
      CONCAT(A.merchant_name, ' (', A.merchant_id, ')') AS merchant_name_with_id
    FROM AllAccounts AS A
  ),
  TotalProducts AS (
    SELECT
      merchant_id,
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
      COUNT(*) AS total_products
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    GROUP BY
      merchant_id,
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
      brand
  ),
  MerchantIds AS (
    SELECT DISTINCT
      aggregator_id,
      merchant_id,
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
      brand
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
  ),
  DisapprovedOffers AS (
    SELECT
      merchant_id,
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
      '% items disapproved' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE is_disapproved
    GROUP BY
      merchant_id,
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
      brand
  ),
  OffersWithBrand AS (
    SELECT
      merchant_id,
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
      '% items that have brand' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      IFNULL(brand, '') != ''
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithGtin AS (
    SELECT
      merchant_id,
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
      '% items that have gtin' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      gtin IS NOT NULL
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithDescription500 AS (
    SELECT
      merchant_id,
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
      '% items with description length >= 500' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      LENGTH(description) >= 500
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithTitle30 AS (
    SELECT
      merchant_id,
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
      '% items with title length >= 30' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      LENGTH(title) >= 30
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithColor AS (
    SELECT
      merchant_id,
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
      '% items with color' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      color IS NOT NULL
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithAgeGroup AS (
    SELECT
      merchant_id,
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
      '% items with age group' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      age_group IS NOT NULL
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithGender AS (
    SELECT
      merchant_id,
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
      '% items with gender' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      gender IS NOT NULL
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithSize AS (
    SELECT
      merchant_id,
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
      '% items with size' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      IFNULL(size, '') != ''
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWith3AdditionalImages AS (
    SELECT
      merchant_id,
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
      '% items with 3 or more additional images' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      ARRAY_LENGTH(additional_image_links) > 2
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithLifestyleImages AS (
    SELECT
      merchant_id,
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
      '% items with lifestyle image' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      ARRAY_LENGTH(lifestyle_image_links) > 0
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithCustomLabel AS (
    SELECT
      merchant_id,
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
      '% items with custom label' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      IFNULL(custom_label_0, '') != ''
      OR IFNULL(custom_label_1, '') != ''
      OR IFNULL(custom_label_2, '') != ''
      OR IFNULL(custom_label_3, '') != ''
      OR IFNULL(custom_label_4, '') != ''
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithSalePrice AS (
    SELECT
      merchant_id,
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
      '% items with sale price' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      IFNULL(sale_price.amount_micros, 0) > 0
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithItemGroupId AS (
    SELECT
      merchant_id,
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
      '% items with item_group_id' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      item_group_id IS NOT NULL
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithRobotsIssue AS (
    SELECT
      merchant_id,
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
      '% items with robots.txt issue' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e LIKE '%page not crawlable due to robots.txt%'
      )
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithGenericImages AS (
    SELECT
      merchant_id,
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
      '% items with generic image' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e = 'Generic image'
      )
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersGoodProductTypeDepth AS (
    SELECT
      merchant_id,
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
      '% items with good product_type depth' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(product_types) AS e
        WHERE ARRAY_LENGTH(SPLIT(e, ' > ')) > 2
      )
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithInvalidGtins AS (
    SELECT
      merchant_id,
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
      '% items with invalid gtins' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE
          e IN (
            'Unsupported value due to restrictions: GTIN [gtin]',
            'Invalid value [gtin]',
            'Incorrect identifier [gtin]',
            'Ambiguous value [gtin]',
            'Invalid product identifier [gtin]',
            'Unsupported value due to restricted prefix [gtin]')
      )
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithClicks AS (
    SELECT
      merchant_id,
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
      '% items with clicks' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      had_clicks
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithImpressions AS (
    SELECT
      merchant_id,
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
      '% items with impressions' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      had_impressions
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersTargeted AS (
    SELECT
      merchant_id,
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
      '% targeted items' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_targeting
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithProductHighlights AS (
    SELECT
      merchant_id,
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
      '% items with product highlight attributes' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      ARRAY_LENGTH(product_highlights) > 0
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  HasFreeListings AS (
    SELECT
      merchant_id,
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
      'has Free Listings enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_free_listings_enabled
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithPriceAvailabilityConditionAIU AS (
    SELECT
      merchant_id,
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
      '% items with price / availability / condition AIU' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e LIKE '%Automatic item updates active%'
      )
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  LiaItemsWithNoInventory AS (
    SELECT
      merchant_id,
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
      'LIA: % items disapproved for missing inventory' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e = 'Missing inventory data'
      )
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  LiaOffersApproved AS (
    SELECT
      merchant_id,
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
      'LIA: % approved items' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      channel = 'local'
      AND NOT is_disapproved
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithShipping AS (
    SELECT
      merchant_id,
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
      '% items with shipping' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      ARRAY_LENGTH(shipping) > 0 OR (has_account_level_shipping AND ARRAY_LENGTH(shipping) = 0)
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithShippingSpeed AS (
    SELECT
      merchant_id,
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
      '% items with shipping speed' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products AS TP
    WHERE
      EXISTS(
        SELECT *
        FROM TP.shipping
        WHERE
          min_handling_time IS NOT NULL
          AND max_handling_time IS NOT NULL
          AND min_transit_time IS NOT NULL
          AND max_transit_time IS NOT NULL
      )
      OR (has_account_level_shipping_speed AND ARRAY_LENGTH(shipping) = 0)
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithFastShipping AS (
    SELECT
      merchant_id,
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
      '% items with fast shipping option' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products AS TP
    WHERE
      EXISTS(
        SELECT *
        FROM TP.shipping
        WHERE
          max_handling_time IS NOT NULL
          AND max_transit_time IS NOT NULL
          AND max_handling_time + max_transit_time <= 3
      )
      OR (has_account_level_fast_shipping AND ARRAY_LENGTH(shipping) = 0)
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OffersWithFreeShipping AS (
    SELECT
      merchant_id,
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
      '% items with free shipping' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products AS TP
    WHERE
      EXISTS(
        SELECT *
        FROM TP.shipping
        WHERE IFNULL(price.amount_micros, 0) = 0
      )
      OR (has_account_level_free_shipping AND ARRAY_LENGTH(shipping) = 0)
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  ImageAiu AS (
    SELECT
      merchant_id,
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
      'image AIU enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_image_aiu_enabled
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  AvailabilityAiu AS (
    SELECT
      merchant_id,
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
      'availability AIU enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_availability_aiu_enabled
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  LiaImplemented AS (
    SELECT
      merchant_id,
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
      'has LIA implemented' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_has_lia_implemented
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  MhlsfImplemented AS (
    SELECT
      merchant_id,
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
      'LIA: has MHLSF implemented' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_has_mhlsf_implemented
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  StorePickupImplemented AS (
    SELECT
      merchant_id,
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
      'LIA: has Store Pickup implemented' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_has_store_pickup_implemented
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  OnDisplayToOrderImplemented AS (
    SELECT
      merchant_id,
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
      'LIA: On display to order implemented' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_has_odo_implemented
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  RawDuplicateTitles AS (
    SELECT
      merchant_id,
      channel,
      targeted_country,
      SPLIT(product_id, ':')[SAFE_OFFSET(1)] AS language_code,
      title,
      COUNT(*) AS duplicate_title_count
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    GROUP BY 1, 2, 3, 4, 5
  ),
  ItemsWithDuplicateTitles AS (
    SELECT
      T.merchant_id,
      T.channel,
      T.targeted_country,
      T.product_type_lvl1,
      T.product_type_lvl2,
      T.product_type_lvl3,
      T.custom_label_0,
      T.custom_label_1,
      T.custom_label_2,
      T.custom_label_3,
      T.custom_label_4,
      T.brand,
      '% items with duplicate titles' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products AS T
    INNER JOIN RawDuplicateTitles AS DT
      ON
        DT.merchant_id = T.merchant_id
        AND DT.channel = T.channel
        AND DT.targeted_country = T.targeted_country
        AND DT.language_code = SPLIT(T.product_id, ':')[SAFE_OFFSET(1)]
        AND DT.title = T.title
    WHERE DT.duplicate_title_count > 1
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  ItemsWithCostOfGoodsSold AS (
    SELECT
      merchant_id,
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
      '% items with cost_of_goods_sold' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE IFNULL(cost_of_goods_sold.amount_micros, 0) > 0
    GROUP BY
      merchant_id,
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
      metric_name
  ),
  IsLiaReadyOrStarted AS (
    SELECT
      merchant_id,
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
      'is LIA ready or started' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_is_lia_ready_or_started
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  IsLiaEnabled AS (
    SELECT
      merchant_id,
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
      'is LIA enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_is_lia_enabled
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasMerchantPromotionsEnabled AS (
    SELECT
      merchant_id,
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
      'has Merchant Promotions enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_merchant_promotions_enabled
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasProductReviewsEnabled AS (
    SELECT
      merchant_id,
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
      'has Product Ratings enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_product_reviews_enabled
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasCheckoutOnMerchantEnabled AS (
    SELECT
      merchant_id,
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
      'has checkout on merchant enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_checkout_on_merchant_enabled
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasCwcdImplemented AS (
    SELECT
      merchant_id,
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
      'has CWCD implemented' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_cwcd_implemented
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasReturnsEnabled AS (
    SELECT
      merchant_id,
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
      'has returns policy enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_returns_enabled
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  ActivePromotionsCountMetric AS (
    SELECT
      merchant_id,
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
      'active promotions last year' AS metric_name,
      MAX(active_promotions_count) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasMarketInsights AS (
    SELECT
      merchant_id,
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
      'has Market Insights enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_market_insights
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  UsesStructuredData AS (
    SELECT
      merchant_id,
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
      'uses structured data' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      uses_structured_data
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  LiaCurbsidePickupImplemented AS (
    SELECT
      merchant_id,
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
      'LIA: has Curbside Pickup implemented' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      lia_has_curbside_pickup_implemented
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  LiaOffersEligible AS (
    SELECT
      merchant_id,
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
      'LIA: % eligible items' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      channel = 'local'
      AND EXISTS(
        SELECT 1
        FROM UNNEST(destination_statuses) AS ds
        WHERE
          ds.reporting_context = 'LOCAL_INVENTORY_ADS'
          AND ARRAY_LENGTH(ds.approved_countries) > 0
      )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  OffersWithShortTitle AS (
    SELECT
      merchant_id,
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
      '% items with short title' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      LENGTH(title) < 15
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  OffersWithUsedCondition AS (
    SELECT
      merchant_id,
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
      '% items with used condition' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      UPPER(IFNULL(condition, 'NEW')) != 'NEW'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  OffersWithUniqueTitles AS (
    SELECT
      T.merchant_id,
      T.channel,
      T.targeted_country,
      T.product_type_lvl1,
      T.product_type_lvl2,
      T.product_type_lvl3,
      T.custom_label_0,
      T.custom_label_1,
      T.custom_label_2,
      T.custom_label_3,
      T.custom_label_4,
      T.brand,
      '% items with unique titles' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products AS T
    LEFT JOIN RawDuplicateTitles AS DT
      ON
        DT.merchant_id = T.merchant_id
        AND DT.channel = T.channel
        AND DT.targeted_country = T.targeted_country
        AND DT.language_code = SPLIT(T.product_id, ':')[SAFE_OFFSET(1)]
        AND DT.title = T.title
    WHERE IFNULL(DT.duplicate_title_count, 1) = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  OffersWithInstallment AS (
    SELECT
      merchant_id,
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
      '% items with installment' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      installment IS NOT NULL AND JSON_VALUE(installment, '$.amount.amount_micros') IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasAccountLevelShipping AS (
    SELECT
      merchant_id,
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
      'has account level shipping' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_account_level_shipping
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasMcaMarketplaceStructure AS (
    SELECT
      merchant_id,
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
      'has MCA marketplace structure' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      aggregator_id != 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasAdsRedirect AS (
    SELECT
      merchant_id,
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
      'has ads_redirect' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_ads_redirect
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasValidVideoLink AS (
    SELECT
      merchant_id,
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
      'has valid video_link' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_valid_video_link
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasLoyaltyPricing AS (
    SELECT
      merchant_id,
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
      'has loyalty member pricing activated' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_loyalty_member_pricing_activated
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  HasAvailableCustomLabels AS (
    SELECT
      merchant_id,
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
      'Has available Custom Labels to drive product relevance' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      IFNULL(custom_label_0, '') != ''
      OR IFNULL(custom_label_1, '') != ''
      OR IFNULL(custom_label_2, '') != ''
      OR IFNULL(custom_label_3, '') != ''
      OR IFNULL(custom_label_4, '') != ''
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  AllMetrics AS (
    SELECT * FROM DisapprovedOffers
    UNION ALL
    SELECT * FROM IsLiaReadyOrStarted
    UNION ALL
    SELECT * FROM IsLiaEnabled
    UNION ALL
    SELECT * FROM HasMerchantPromotionsEnabled
    UNION ALL
    SELECT * FROM HasProductReviewsEnabled
    UNION ALL
    SELECT * FROM HasCheckoutOnMerchantEnabled
    UNION ALL
    SELECT * FROM HasCwcdImplemented
    UNION ALL
    SELECT * FROM HasReturnsEnabled
    UNION ALL
    SELECT * FROM ActivePromotionsCountMetric
    UNION ALL
    SELECT * FROM HasMarketInsights
    UNION ALL
    SELECT * FROM UsesStructuredData
    UNION ALL
    SELECT * FROM LiaCurbsidePickupImplemented
    UNION ALL
    SELECT * FROM LiaOffersEligible
    UNION ALL
    SELECT * FROM OffersWithBrand
    UNION ALL
    SELECT * FROM OffersWithGtin
    UNION ALL
    SELECT * FROM OffersWithDescription500
    UNION ALL
    SELECT * FROM OffersWithTitle30
    UNION ALL
    SELECT * FROM OffersWithColor
    UNION ALL
    SELECT * FROM OffersWithAgeGroup
    UNION ALL
    SELECT * FROM OffersWithGender
    UNION ALL
    SELECT * FROM OffersWithSize
    UNION ALL
    SELECT * FROM OffersWith3AdditionalImages
    UNION ALL
    SELECT * FROM OffersWithCustomLabel
    UNION ALL
    SELECT * FROM OffersWithSalePrice
    UNION ALL
    SELECT * FROM OffersWithItemGroupId
    UNION ALL
    SELECT * FROM OffersWithRobotsIssue
    UNION ALL
    SELECT * FROM OffersWithGenericImages
    UNION ALL
    SELECT * FROM OffersGoodProductTypeDepth
    UNION ALL
    SELECT * FROM OffersWithInvalidGtins
    UNION ALL
    SELECT * FROM OffersWithClicks
    UNION ALL
    SELECT * FROM OffersWithImpressions
    UNION ALL
    SELECT * FROM OffersTargeted
    UNION ALL
    SELECT * FROM OffersWithProductHighlights
    UNION ALL
    SELECT * FROM HasFreeListings
    UNION ALL
    SELECT * FROM OffersWithPriceAvailabilityConditionAIU
    UNION ALL
    SELECT * FROM LiaItemsWithNoInventory
    UNION ALL
    SELECT * FROM LiaOffersApproved
    UNION ALL
    SELECT * FROM OffersWithShipping
    UNION ALL
    SELECT * FROM OffersWithShippingSpeed
    UNION ALL
    SELECT * FROM OffersWithFastShipping
    UNION ALL
    SELECT * FROM OffersWithFreeShipping
    UNION ALL
    SELECT * FROM ImageAiu
    UNION ALL
    SELECT * FROM AvailabilityAiu
    UNION ALL
    SELECT * FROM LiaImplemented
    UNION ALL
    SELECT * FROM MhlsfImplemented
    UNION ALL
    SELECT * FROM StorePickupImplemented
    UNION ALL
    SELECT * FROM OnDisplayToOrderImplemented
    UNION ALL
    SELECT * FROM ItemsWithDuplicateTitles
    UNION ALL
    SELECT * FROM ItemsWithCostOfGoodsSold
    UNION ALL
    SELECT * FROM OffersWithShortTitle
    UNION ALL
    SELECT * FROM OffersWithUsedCondition
    UNION ALL
    SELECT * FROM OffersWithUniqueTitles
    UNION ALL
    SELECT * FROM OffersWithInstallment
    UNION ALL
    SELECT * FROM HasAccountLevelShipping
    UNION ALL
    SELECT * FROM HasMcaMarketplaceStructure
    UNION ALL
    SELECT * FROM HasAdsRedirect
    UNION ALL
    SELECT * FROM HasValidVideoLink
    UNION ALL
    SELECT * FROM HasLoyaltyPricing
    UNION ALL
    SELECT * FROM HasAvailableCustomLabels
  ),
  BenchmarksPerMerchant AS (
    SELECT * FROM MerchantIds, Benchmarks
  )
SELECT
  CURRENT_DATE() AS extraction_date,
  CAST(BM.merchant_id AS STRING) AS merchant_id,
  AN.merchant_name,
  CAST(BM.aggregator_id AS STRING) AS aggregator_id,
  AN.aggregator_name,
  AN.merchant_name_with_id,
  TP.channel,
  TP.targeted_country,
  TP.product_type_lvl1,
  TP.product_type_lvl2,
  TP.product_type_lvl3,
  TP.custom_label_0,
  TP.custom_label_1,
  TP.custom_label_2,
  TP.custom_label_3,
  TP.custom_label_4,
  TP.brand,
  BM.metric_name,
  BM.benchmark,
  BM.comparison_type,
  BM.metric_level,
  BM.description,
  BM.support_link,
  BM.metric_category,
  BM.priority,
  BM.lia_metric,
  IF(BM.lia_metric, 'Uncheck this one to remove LIA metrics', 'Check both to include LIA metrics')
    AS lia_settings,
  AM.metric_value,
  TP.total_products
FROM BenchmarksPerMerchant AS BM
INNER JOIN
  TotalProducts AS TP
  ON
    BM.merchant_id = TP.merchant_id
    AND BM.channel = TP.channel
    AND BM.targeted_country = TP.targeted_country
    AND BM.product_type_lvl1 = TP.product_type_lvl1
    AND BM.product_type_lvl2 = TP.product_type_lvl2
    AND BM.product_type_lvl3 = TP.product_type_lvl3
    AND BM.custom_label_0 = TP.custom_label_0
    AND BM.custom_label_1 = TP.custom_label_1
    AND BM.custom_label_2 = TP.custom_label_2
    AND BM.custom_label_3 = TP.custom_label_3
    AND BM.custom_label_4 = TP.custom_label_4
    AND BM.brand = TP.brand
LEFT JOIN
  AllMetrics AS AM
  ON
    BM.merchant_id = AM.merchant_id
    AND BM.channel = AM.channel
    AND BM.targeted_country = AM.targeted_country
    AND BM.product_type_lvl1 = AM.product_type_lvl1
    AND BM.product_type_lvl2 = AM.product_type_lvl2
    AND BM.product_type_lvl3 = AM.product_type_lvl3
    AND BM.custom_label_0 = AM.custom_label_0
    AND BM.custom_label_1 = AM.custom_label_1
    AND BM.custom_label_2 = AM.custom_label_2
    AND BM.custom_label_3 = AM.custom_label_3
    AND BM.custom_label_4 = AM.custom_label_4
    AND BM.brand = AM.brand
    AND BM.metric_name = AM.metric_name
LEFT JOIN
  AccountNames AS AN
  ON BM.merchant_id = AN.merchant_id;