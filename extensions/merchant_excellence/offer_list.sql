CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_Offer_List
  PARTITION BY
    extraction_date
  OPTIONS (
    partition_expiration_days = 60)
AS
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
      PSC.product_id,
      PSC.targeted_country,
      PSC.is_disapproved,
      PSC.item_issues,
      PSC.has_free_listings_enabled,
      PSC.has_dynamic_remarketing_enabled,
      P.product.offer_id AS item_id,
      P.product.content_language AS language,
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
      IFNULL(AD.clicks_last30days, 0) > 0 AS had_clicks
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
  ),
  DisapprovedOffers AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items disapproved' AS metric_name,
      'item disapproved' AS data_quality_flag,
    FROM Products
    WHERE is_disapproved
  ),
  OffersWithBrand AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items that have brand' AS metric_name,
      'no brand' AS data_quality_flag,
    FROM Products
    WHERE
      IFNULL(brand, '') = ''
  ),
  OffersWithGtin AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items that have gtin' AS metric_name,
      'no gtin' AS data_quality_flag,
    FROM Products
    WHERE
      gtin IS NULL
  ),
  OffersWithDescription500 AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with description length >= 500' AS metric_name,
      'description length < 500' AS data_quality_flag,
    FROM Products
    WHERE
      LENGTH(description) < 500
  ),
  OffersWithTitle30 AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with title length >= 30' AS metric_name,
      'title length < 30' AS data_quality_flag,
    FROM Products
    WHERE
      LENGTH(title) < 30
  ),
  OffersWithColor AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with color' AS metric_name,
      'no color' AS data_quality_flag,
    FROM Products
    WHERE
      color IS NULL
  ),
  OffersWithAgeGroup AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with age group' AS metric_name,
      'no age group' AS data_quality_flag,
    FROM Products
    WHERE
      age_group IS NULL
  ),
  OffersWithGender AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with gender' AS metric_name,
      'no gender' AS data_quality_flag,
    FROM Products
    WHERE
      gender IS NULL
  ),
  OffersWithSize AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with size' AS metric_name,
      'no size' AS data_quality_flag,
    FROM Products
    WHERE
      -- TODO: check if this logic works for offers without Sizes
      -- Currently all offers in this test feed have the attribute
      ARRAY_LENGTH(IFNULL(sizes, [])) = 0
  ),
  OffersWithAdditionalImages AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with additional images' AS metric_name,
      'no additional images' AS data_quality_flag,
    FROM Products
    WHERE
      ARRAY_LENGTH(IFNULL(additional_image_links, [])) = 0
  ),
  OffersWithCustomLabel AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with custom label' AS metric_name,
      'no custom labels' AS data_quality_flag,
    FROM Products
    WHERE
      IFNULL(custom_label_0, '') = ''
      AND IFNULL(custom_label_1, '') = ''
      AND IFNULL(custom_label_2, '') = ''
      AND IFNULL(custom_label_3, '') = ''
      AND IFNULL(custom_label_4, '') = ''
  ),
  OffersWithSalePrice AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with sale price' AS metric_name,
      'no sale price' AS data_quality_flag,
    FROM Products
    WHERE
      IFNULL(CAST(sale_price.value AS FLOAT64), 0) = 0
  ),
  OffersWithItemGroupId AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with item_group_id' AS metric_name,
      'no item_group_id' AS data_quality_flag,
    FROM Products
    WHERE
      item_group_id IS NULL
  ),
  OffersWithRobotsIssue AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with robots.txt issue' AS metric_name,
      'robots.txt issue' AS data_quality_flag,
    FROM Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e LIKE '%page not crawlable due to robots.txt%'
      )
  ),
  OffersWithGenericImages AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with generic image' AS metric_name,
      'generic image' AS data_quality_flag,
    FROM Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e = 'Generic image'
      )
  ),
  OffersGoodProductTypeDepth AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with good product_type depth' AS metric_name,
      'product_type depth < 3' AS data_quality_flag,
    FROM Products
    WHERE
      NOT EXISTS(
        SELECT 1
        FROM UNNEST(product_types) AS e
        WHERE ARRAY_LENGTH(SPLIT(e, ' > ')) > 2
      )
  ),
  OffersWithInvalidGtins AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with invalid gtins' AS metric_name,
      'invalid gtin' AS data_quality_flag,
    FROM Products
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
  ),
  OffersWithClicks AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with clicks' AS metric_name,
      'no clicks' AS data_quality_flag,
    FROM Products
    WHERE
      NOT had_clicks
  ),
  OffersWithImpressions AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with impressions' AS metric_name,
      'no impressions' AS data_quality_flag,
    FROM Products
    WHERE
      NOT had_impressions
  ),
  OffersTargeted AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% targeted offers' AS metric_name,
      'not targeted' AS data_quality_flag,
    FROM Products
    WHERE
      NOT has_targeting
  ),
  OffersWithProductHighlights AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with product highlight' AS metric_name,
      'no product highlight' AS data_quality_flag,
    FROM Products
    WHERE
      ARRAY_LENGTH(IFNULL(product_highlights, [])) = 0
  ),
  OffersWithPriceAvailabilityAIU AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with price/availability AIU' AS metric_name,
      'price/availability AIU active' AS data_quality_flag,
    FROM Products
    WHERE
      EXISTS(
        SELECT 1
        FROM UNNEST(item_issues) AS e
        WHERE e LIKE '%Automatic item updates active%'
      )
  ),
  LiaOffersApproved AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      'LIA: % approved offers' AS metric_name,
      'LIA offer disapproved' AS data_quality_flag,
    FROM Products
    WHERE
      channel = 'local'
      AND is_disapproved
  ),
  OffersUploadedViaApi AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      'products uploaded via API' AS metric_name,
      'product not upload via API' AS data_quality_flag,
    FROM Products
    WHERE
      source != 'api'
  ),
  OffersWithShipping AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      channel,
      item_id,
      targeted_country,
      language,
      '% items with shipping attribute' AS metric_name,
      'no shipping attribute' AS data_quality_flag,
    FROM Products
    WHERE
      ARRAY_LENGTH(IFNULL(shipping, [])) = 0
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
    SELECT * FROM OffersWithPriceAvailabilityAIU
    UNION ALL
    SELECT * FROM LiaOffersApproved
    UNION ALL
    SELECT * FROM OffersUploadedViaApi
    UNION ALL
    SELECT * FROM OffersWithShipping
  )
SELECT
  CURRENT_DATE() AS extraction_date,
  CAST(P.merchant_id AS STRING) AS merchant_id,
  AN.merchant_name,
  CAST(P.aggregator_id AS STRING) AS aggregator_id,
  AN.aggregator_name,
  AN.merchant_name_with_id,
  P.channel,
  P.item_id,
  P.targeted_country,
  P.language,
  P.title,
  P.product_type_lvl1,
  P.product_type_lvl2,
  P.product_type_lvl3,
  P.custom_label_0,
  P.custom_label_1,
  P.custom_label_2,
  P.custom_label_3,
  P.custom_label_4,
  P.brand,
  AM.metric_name,
  AM.data_quality_flag
FROM Products AS P
INNER JOIN AllMetrics AS AM
  USING (
    merchant_id,
    channel,
    item_id,
    targeted_country,
    language)
LEFT JOIN AccountNames AS AN
  USING (merchant_id);