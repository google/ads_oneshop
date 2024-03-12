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
  Lia AS (
    SELECT DISTINCT
      C.account_id AS merchant_id,
      EXISTS(
        SELECT 1
        FROM C.country_settings
        WHERE
          inventory.status = 'active'
          AND inventory.inventory_verification_contact_status = 'active'
          AND about.status = 'active'
      ) AS lia_has_lia_implemented,
      EXISTS(
        SELECT 1
        FROM C.country_settings
        WHERE
          hosted_local_storefront_active
          OR omnichannel_experience.lsf_type IN ('mhlsfBasic', 'mhlsfFull')
      ) AS lia_has_mhlsf_implemented,
      EXISTS(
        SELECT 1
        FROM C.country_settings
        WHERE
          store_pickup_active
          OR ARRAY_LENGTH(omnichannel_experience.pickup_types) > 0
      ) AS lia_has_store_pickup_implemented,
      EXISTS(
        SELECT 1
        FROM C.country_settings
        WHERE on_display_to_order.status = 'active'
      ) AS lia_has_odo_implemented
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.liasettings AS L,
      L.children AS C
  ),
  AccountLevelShipping AS (
    SELECT DISTINCT
      accountId AS merchant_id,
      ARRAY_LENGTH(services) > 0 AS has_account_level_shipping
    FROM ${PROJECT_NAME}.${DATASET_NAME}.shippingsettings
  ),
  Account AS (
    SELECT DISTINCT
      C.id AS merchant_id,
      A.settings.id AS aggregator_id,
      IFNULL(
        C.automaticImprovements.imageImprovements.effectiveAllowAutomaticImageImprovements,
        A.settings.automaticImprovements.imageImprovements.effectiveAllowAutomaticImageImprovements)
        AS has_image_aiu_enabled,
      IFNULL(
        (
          C.automaticImprovements.itemUpdates.effectiveAllowStrictAvailabilityUpdates
          OR C.automaticImprovements.itemUpdates.effectiveAllowAvailabilityUpdates),
        (
          A.settings.automaticImprovements.itemUpdates.effectiveAllowStrictAvailabilityUpdates
          OR A.settings.automaticImprovements.itemUpdates.effectiveAllowAvailabilityUpdates))
        AS has_availability_aiu_enabled,
      L.lia_has_lia_implemented,
      L.lia_has_mhlsf_implemented,
      L.lia_has_store_pickup_implemented,
      L.lia_has_odo_implemented,
      ALS.has_account_level_shipping
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A,
      A.children AS C
    LEFT JOIN Lia AS L
      ON L.merchant_id = C.id
    LEFT JOIN AccountLevelShipping AS ALS
      ON ALS.merchant_id = C.id
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
  PSC.has_dynamic_remarketing_enabled,
  P.product.offer_id AS item_id,
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
  P.product.gtin,
  P.product.description,
  P.product.title,
  P.product.color,
  P.product.age_group,
  P.product.gender,
  P.product.sizes,
  P.product.additional_image_links,
  P.product.sale_price,
  P.product.item_group_id,
  P.product.product_types,
  P.product.product_highlights,
  P.product.source,
  P.product.shipping,
  (P.has_shopping_targeting OR P.has_performance_max_targeting) AS has_targeting,
  IFNULL(AD.impressions_last30days, 0) > 0 AS had_impressions,
  IFNULL(AD.clicks_last30days, 0) > 0 AS had_clicks,
  AC.has_image_aiu_enabled,
  AC.has_availability_aiu_enabled,
  AC.lia_has_lia_implemented,
  AC.lia_has_mhlsf_implemented,
  AC.lia_has_store_pickup_implemented,
  AC.lia_has_odo_implemented,
  AC.has_account_level_shipping
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
  AccountNames AS (
    SELECT DISTINCT
      C.id AS merchant_id,
      C.name AS merchant_name,
      A.settings.id AS aggregator_id,
      A.settings.name AS aggregator_name,
      CONCAT(C.name, ' (', C.id, ')') AS merchant_name_with_id
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A, A.children AS C
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
      -- TODO: check if this logic works for offers without Sizes
      -- Currently all offers in this test feed have the attribute
      ARRAY_LENGTH(sizes) > 0
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
  OffersWithAdditionalImages AS (
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
      '% items with additional images' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      ARRAY_LENGTH(additional_image_links) > 0
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
      CAST(sale_price.value AS FLOAT64) > 0
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
      '% targeted offers' AS metric_name,
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
      '% items with product highlight' AS metric_name,
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
  HasDynamicRemarketing AS (
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
      'has Dynamic Remarketing enabled' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_dynamic_remarketing_enabled
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
  OffersWithPriceAvailabilityAIU AS (
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
      '% items with price/availability AIU' AS metric_name,
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
      'LIA: % approved offers' AS metric_name,
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
  OffersUploadedViaApi AS (
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
      'products uploaded via API' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      source = 'api'
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
      '% items with shipping attribute' AS metric_name,
      COUNT(*) AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      ARRAY_LENGTH(shipping) > 0
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
  AccountLevelShippingImplemented AS (
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
      'uses account-level shipping settings' AS metric_name,
      1 AS metric_value
    FROM ${PROJECT_NAME}.${DATASET_NAME}._tmp_Products
    WHERE
      has_account_level_shipping
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
  AllMetrics AS (
    SELECT * FROM DisapprovedOffers
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
    SELECT * FROM OffersWithAdditionalImages
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
    SELECT * FROM HasDynamicRemarketing
    UNION ALL
    SELECT * FROM HasFreeListings
    UNION ALL
    SELECT * FROM OffersWithPriceAvailabilityAIU
    UNION ALL
    SELECT * FROM LiaOffersApproved
    UNION ALL
    SELECT * FROM OffersUploadedViaApi
    UNION ALL
    SELECT * FROM OffersWithShipping
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
    SELECT * FROM AccountLevelShippingImplemented
    UNION ALL
    SELECT * FROM OnDisplayToOrderImplemented
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
  ON BM.merchant_id = TP.merchant_id
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
  ON BM.merchant_id = AM.merchant_id
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
