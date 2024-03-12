CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_Account_List
  PARTITION BY
    extraction_date
  OPTIONS (
    partition_expiration_days = 90)
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
  EnabledDestinations AS (
    SELECT DISTINCT
      CAST(account_id AS INT64) AS merchant_id,
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
      C.name AS merchant_name,
      A.settings.id AS aggregator_id,
      A.settings.name AS aggregator_name,
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
      ALS.has_account_level_shipping,
      ED.has_free_listings_enabled,
      ED.has_dynamic_remarketing_enabled
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A,
      A.children AS C
    LEFT JOIN Lia AS L
      ON L.merchant_id = C.id
    LEFT JOIN AccountLevelShipping AS ALS
      ON ALS.merchant_id = C.id
    LEFT JOIN EnabledDestinations AS ED
      ON ED.merchant_id = C.id
  ),
  HasDynamicRemarketing AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'has Dynamic Remarketing enabled' AS metric_name,
      'dynamic remarketing not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_dynamic_remarketing_enabled
  ),
  HasFreeListings AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'has Free Listings enabledd' AS metric_name,
      'free listings not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_free_listings_enabled
  ),
  ImageAiu AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'image AIU enabled' AS metric_name,
      'image AIU not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_image_aiu_enabled
  ),
  AvailabilityAiu AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'availability AIU enabled' AS metric_name,
      'availability AIU not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_availability_aiu_enabled
  ),
  LiaImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'has LIA implemented' AS metric_name,
      'lia not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_lia_implemented
  ),
  MhlsfImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'LIA: has MHLSF implemented' AS metric_name,
      'mhlsf not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_mhlsf_implemented
  ),
  StorePickupImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'LIA: has Store Pickup implemented' AS metric_name,
      'store pickup not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_store_pickup_implemented
  ),
  OnDisplayToOrderImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'LIA: On display to order implemented' AS metric_name,
      'on display to order not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_odo_implemented
  ),
  AccountLevelShippingImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'uses account-level shipping settings' AS metric_name,
      'account-level shipping not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT has_account_level_shipping
  ),
  AllMetrics AS (
    SELECT * FROM HasDynamicRemarketing
    UNION ALL
    SELECT * FROM HasFreeListings
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
  )
SELECT DISTINCT
  CURRENT_DATE('UTC') AS extraction_date,
  CAST(AM.merchant_id AS STRING) AS merchant_id,
  AN.merchant_name,
  CAST(AM.aggregator_id AS STRING) AS aggregator_id,
  AN.aggregator_name,
  AN.merchant_name_with_id,
  metric_name,
  data_quality_flag
FROM AllMetrics AS AM
LEFT JOIN AccountNames AS AN
  USING (merchant_id);
