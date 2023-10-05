import google.cloud.bigquery as bigquery
from google.api_core.exceptions import NotFound
from google.cloud.exceptions import NotFound
import acit.config.constants as constants

_PROJECT_NAME = constants.PROJECT_NAME


def create_product_status_table(client, dataset, storage_bucket, table_name):
  dataset_ref = client.dataset(dataset)
  table_ref = dataset_ref.table(table_name)

  try:
    table = client.get_table(table_ref)
    print("table {} already exists.".format(table))
  except NotFound:
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("kind", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("productId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "destinationStatuses",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("destination",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("approvedCountries",
                                         "STRING",
                                         mode="REPEATED"),
                    bigquery.SchemaField("pendingCountries",
                                         "STRING",
                                         mode="REPEATED"),
                    bigquery.SchemaField("disapprovedCountries",
                                         "STRING",
                                         mode="REPEATED"),
                ],
            ),
            bigquery.SchemaField("creationDate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lastUpdateDate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("googleExpirationDate",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField(
                "itemLevelIssues",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("servability",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("resolution",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("attributeName",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("destination",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("description",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("detail", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("documentation",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("applicableCountries",
                                         "STRING",
                                         mode="REPEATED"),
                ],
            ),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # Note: reference to file is gs://{bucket_name}/{file_name}
    uri = "gs://{}/Product_Status_Storage_Data".format(storage_bucket)
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    print("Loaded {} rows.".format(table.num_rows))
  return table


def create_products_table(client, dataset, storage_bucket, table_name):
  dataset_ref = client.dataset(dataset)
  table_ref = dataset_ref.table(table_name)

  try:
    table = client.get_table(table_ref)
    print("table {} already exists.".format(table))
  except NotFound:
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("kind", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("merchantId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("aggregatorId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("offerId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("identifierExists", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("imageLink", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("additionalImageLinks",
                                 "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField("lifestyleImageLinks",
                                 "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField("contentLanguage", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("targetCountry", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("feedLabel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("channel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("expirationDate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("disclosureDate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("adult", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("availability", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("condition", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("googleProductCategory",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("gtin", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("material", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("mpn", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pattern", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("salePriceEffectiveDate",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("sizes", "STRING", mode="REPEATED"),
            bigquery.SchemaField("sizeSystem", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sizeType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("itemGroupId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "shipping",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(
                        "price",
                        "RECORD",
                        mode="NULLABLE",
                        fields=[
                            bigquery.SchemaField("currency",
                                                 "STRING",
                                                 mode="NULLABLE"),
                            bigquery.SchemaField("value",
                                                 "STRING",
                                                 mode="NULLABLE"),
                        ],
                    ),
                    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("service", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("locationId",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("locationGroupName",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("postalCode",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("minHandlingTime",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("maxHandlingTime",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("minTransitTime",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("maxTransitTime",
                                         "STRING",
                                         mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("multipack", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "customAttributes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("groupValues", "JSON",
                                         mode="REPEATED"),
                ],
            ),
            bigquery.SchemaField("customLabel0", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customLabel1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customLabel2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customLabel3", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customLabel4", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("mobileLink", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("availabilityDate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shippingLabel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("isBundle", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("displayAdsId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("displayAdsSimilarIds",
                                 "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField("displayAdsTitle", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("displayAdsLink", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("displayAdsValue", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sellOnGoogleQuantity",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("promotionIds", "STRING", mode="REPEATED"),
            bigquery.SchemaField("maxHandlingTime", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("minHandlingTime", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "installment",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("months", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "price",
                        "RECORD",
                        mode="NULLABLE",
                        fields=[
                            bigquery.SchemaField("currency",
                                                 "STRING",
                                                 mode="NULLABLE"),
                            bigquery.SchemaField("value",
                                                 "NUMERIC",
                                                 mode="NULLABLE"),
                        ],
                    ),
                ],
            ),
            bigquery.SchemaField(
                "loyaltyPoints",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("pointsValue",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("ratio", "NUMERIC", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "unitPricingMeasure",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "NUMERIC", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "unitPricingBaseMeasure",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "costOfGoodsSold",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "NUMERIC", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("includedDestinations",
                                 "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField("excludedDestinations",
                                 "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField("adsGrouping", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("adsLabels", "STRING", mode="REPEATED"),
            bigquery.SchemaField("adsRedirect", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("productTypes", "STRING", mode="REPEATED"),
            bigquery.SchemaField("ageGroup", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("additionalSizeType",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("energyEfficiencyClass",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("minEnergyEfficiencyClass",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("maxEnergyEfficiencyClass",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("taxCategory", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transitTimeLabel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shoppingAdsExcludedCountries",
                                 "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField("pickupMethod", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pickupSla", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("linkTemplate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "productDetails",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("sectionName",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("attributeName",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("attributeValue",
                                         "STRING",
                                         mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("productHighlights", "STRING",
                                 mode="REPEATED"),
            bigquery.SchemaField(
                "subscriptionCost",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("period", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("periodLength",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField(
                        "amount",
                        "RECORD",
                        mode="NULLABLE",
                        fields=[
                            bigquery.SchemaField("currency",
                                                 "STRING",
                                                 mode="NULLABLE"),
                            bigquery.SchemaField("value",
                                                 "NUMERIC",
                                                 mode="NULLABLE"),
                        ],
                    ),
                ],
            ),
            bigquery.SchemaField("canonicalLink", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("externalSellerId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pause", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "cloudExportAdditionalProperties",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("propertyName",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("textValue", "STRING",
                                         mode="REPEATED"),
                    bigquery.SchemaField("boolValue", "BOOL", mode="NULLABLE"),
                    bigquery.SchemaField("intValue", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("floatValue",
                                         "NUMERIC",
                                         mode="REPEATED"),
                    bigquery.SchemaField("minValue", "NUMERIC",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("maxValue", "NUMERIC",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("unitCode", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "productHeight",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "productLength",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "productWidth",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "productWeight",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "shippingHeight",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "shippingLength",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "price",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "NUMERIC", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "salePrice",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "NUMERIC", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "shippingWeight",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "shippingWidth",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("value", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "taxes",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("rate", "NUMERIC", mode="NULLABLE"),
                    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("taxShip", "BOOL", mode="NULLABLE"),
                    bigquery.SchemaField("locationId",
                                         "STRING",
                                         mode="NULLABLE"),
                    bigquery.SchemaField("postalCode",
                                         "STRING",
                                         mode="NULLABLE"),
                ],
            ),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # Note: reference to file is gs://{bucket_name}/{file_name}
    uri = "gs://{}/Products_Storage_Data".format(storage_bucket)
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    print("Loaded {} rows.".format(table.num_rows))
  return table


# Function to create a dataset in Table
def create_ads_table(client, dataset, storage_bucket, table_name):
  dataset_ref = client.dataset(dataset)
  table_ref = dataset_ref.table(table_name)

  try:
    table = client.get_table(table_ref)
    print("table {} already exists.".format(table))
  except NotFound:
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("manager_id", "INTEGER"),
        bigquery.SchemaField("customer_id", "INTEGER"),
        bigquery.SchemaField("campaign_id", "INTEGER"),
        bigquery.SchemaField("product_item_id", "STRING"),
        bigquery.SchemaField("metrics_clicks", "INTEGER"),
        bigquery.SchemaField("metrics_conversions", "FLOAT"),
        bigquery.SchemaField("metrics_conversions_value", "FLOAT"),
        bigquery.SchemaField("metrics_impressions", "INTEGER"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("product_aggregator_id", "INTEGER"),
        bigquery.SchemaField("product_bidding_category_level1", "STRING"),
        bigquery.SchemaField("product_bidding_category_level2", "STRING"),
        bigquery.SchemaField("product_bidding_category_level3", "STRING"),
        bigquery.SchemaField("product_bidding_category_level4", "STRING"),
        bigquery.SchemaField("product_bidding_category_level5", "STRING"),
        bigquery.SchemaField("product_brand", "STRING"),
        bigquery.SchemaField("product_country", "STRING"),
        bigquery.SchemaField("product_custom_attribute_0", "STRING"),
        bigquery.SchemaField("product_custom_attribute_1", "STRING"),
        bigquery.SchemaField("product_custom_attribute_2", "STRING"),
        bigquery.SchemaField("product_custom_attribute_3", "STRING"),
        bigquery.SchemaField("product_custom_attribute_4", "STRING"),
        bigquery.SchemaField("product_merchant_id", "INTEGER"),
        bigquery.SchemaField("product_store_id", "STRING"),
        bigquery.SchemaField("product_type_l1", "STRING"),
        bigquery.SchemaField("product_type_l2", "STRING"),
        bigquery.SchemaField("product_type_l3", "STRING"),
        bigquery.SchemaField("product_type_l4", "STRING"),
        bigquery.SchemaField("product_type_l5", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("campaign_advertising_channel_type", "INTEGER"),
        bigquery.SchemaField("campaign_advertising_channel_sub_type",
                             "INTEGER"),
        bigquery.SchemaField("campaign_status", "INTEGER"),
        bigquery.SchemaField("product_channel", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("product_language", "STRING"),
    ])

    # NOTE: reference to file is gs://{bucket_name}/{file_name}
    uri = f"gs://{storage_bucket}/Ads_Storage_Data"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    print("Loaded {} rows.".format(table.num_rows))
  return table


def create_geo_table(client, dataset, storage_bucket, table_name):
  dataset_ref = client.dataset(dataset)
  table_ref = dataset_ref.table(table_name)

  try:
    table = client.get_table(table_ref)
    print("table {} already exists.".format(table))
  except NotFound:
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    # NOTE: reference to file is gs://{bucket_name}/{file_name}
    uri = f"gs://{storage_bucket}/Geo_Storage_Data"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    print("Loaded {} rows.".format(table.num_rows))
  return table


def create_languages_table(client, dataset, storage_bucket, table_name):
  dataset_ref = client.dataset(dataset)
  table_ref = dataset_ref.table(table_name)

  try:
    table = client.get_table(table_ref)
    print("table {} already exists.".format(table))
  except NotFound:
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    # NOTE: reference to file is gs://{bucket_name}/{file_name}
    uri = f"gs://{storage_bucket}/Languages_Storage_Data"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    print("Loaded {} rows.".format(table.num_rows))
  return table


def create_ads_final_table(client, project_name, dataset):
  table_id = "{}.{}.ACIT_Joined_Ads_Data".format(project_name, dataset)
  job_config = bigquery.QueryJobConfig(destination=table_id)
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

  sql = f"""
      WITH
      Ads_With_Geo_Lang AS (
      SELECT
          *,
          CONCAT(channel, ':', Language_Code, ':', Country_Code, ':', product_item_id) AS prodId
      FROM (
          SELECT
          AD.* EXCEPT(country,
              product_language,
              product_channel),
          GT.Country_Code,
          LC.Language_code,
          CASE
              WHEN AD.product_channel = '0' THEN 'unspecified'
              WHEN AD.product_channel = '1' THEN 'unknown'
              WHEN AD.product_channel = '2' THEN 'online'
              WHEN AD.product_channel = '3' THEN 'local'
          END
          AS channel,
          FROM
          `{dataset}.ACIT_Ads_Data` AS AD
          LEFT JOIN
          `{dataset}.Geo_Data` AS GT
          ON
          AD.product_country = CONCAT("geoTargetconstantss/",GT.Criteria_ID)
          LEFT JOIN
          `{dataset}.Languages_Data` AS LC
          ON
          AD.product_language = CONCAT("languageconstantss/",LC.Criterion_ID)) )
      SELECT
      *
      FROM (
      WITH
          added_row_number AS (
          SELECT
          * EXCEPT (channel,
              Country_Code,
              Language_code),
          ROW_NUMBER() OVER(PARTITION BY prodId ORDER BY date DESC) AS row_number
          FROM
          Ads_With_Geo_Lang )
      SELECT
          * EXCEPT (metrics_clicks,
          metrics_conversions,
          metrics_conversions_value,
          metrics_impressions)
      FROM
          added_row_number
      WHERE
          row_number = 1) AS Table_A
      INNER JOIN (
      SELECT
          prodId AS reference_prodId,
          SUM(metrics_conversions) AS Conversions,
          SUM(metrics_clicks) AS Clicks,
          SUM(metrics_impressions) AS Impressions
      FROM
          Ads_With_Geo_Lang
      GROUP BY
          reference_prodId ) AS Table_B
      ON
      Table_A.prodId = Table_B.reference_prodId
        """

  query_job = client.query(sql, job_config=job_config)
  query_job.result()

  print("Query Results loaded into table {}".format(table_id))


def create_products_final_table(client, project_name, dataset):
  table_id = "{}.{}.ACIT_Joined_Product_Data".format(project_name, dataset)
  job_config = bigquery.QueryJobConfig(destination=table_id)
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

  sql = f"""
      SELECT * EXCEPT(kind, link) FROM `{_PROJECT_NAME}.{dataset}.ACIT_Products_Data` AS C
      LEFT JOIN
      (
          SELECT
          A.productId as productId,
          A.creationDate,
          A.lastUpdateDate,
          A.googleExpirationDate,
          
          --Issues Breakdown
          STRING_AGG(ILI.servability) AS Issue_Servability,
          COALESCE(STRING_AGG(ILI.code, ", "), "UNAFFECTED") AS Issue_Code,
          COALESCE(STRING_AGG(ILI.description, ", "), "NOTHING") AS Issue_Description,
          COALESCE(STRING_AGG(ILI.attributeName, ", "), "NOTHING") AS Issue_Attributes,
          TO_JSON_STRING(ILI.applicableCountries) as Issue_ApplicableCountries,

          --Destinations Breakdown
          COALESCE(STRING_AGG(DS.destination, ", "), "NOTHING") AS  Destination_Destination,
          COALESCE(STRING_AGG(DS.status, " , "), "NOTHING") AS IsDestination_Status,
          TO_JSON_STRING(DS.approvedCountries) as Destination_Approved,
          TO_JSON_STRING(DS.pendingCountries) as Destination_Pending,
          TO_JSON_STRING(DS.disapprovedCountries) as Destination_Disapproved
      FROM
          `{_PROJECT_NAME}.{dataset}.ACIT_ProductStatus_Data` AS A,
          UNNEST(A.itemLevelIssues) AS ILI,
          UNNEST(A.destinationStatuses) AS DS
      WHERE
          A.lastUpdateDate = (
              SELECT
              MAX(lastUpdateDate)
              FROM
                  `{_PROJECT_NAME}.{dataset}.ACIT_ProductStatus_Data` AS B
              WHERE
                  A.productId = B.productId )
      GROUP BY 
          A.productId,
          A.creationDate,
          A.lastUpdateDate,
          A.googleExpirationDate,
          Destination_Approved,
          Destination_Pending,
          Destination_Disapproved,
          Issue_ApplicableCountries
      ) AS B
      ON 
      C.id = B.productId
      """

  query_job = client.query(sql, job_config=job_config)
  query_job.result()

  print("Query Results loaded into table {}".format(table_id))


def create_final_table(client, project_name, dataset):
  table_id = "{}.{}.ACIT_FINAL_TABLE".format(project_name, dataset)
  job_config = bigquery.QueryJobConfig(destination=table_id,)
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

  sql = f"""
    SELECT * EXCEPT(productId),
    ARRAY_TO_STRING(productTypes, ', ') as product_type,
    id as productId,
    (CASE
        WHEN CONTAINS_SUBSTR(Issue_Servability, 'disapproved') THEN FALSE
        ELSE
        TRUE
    END
        ) AS Is_Approved,
    (CASE
        WHEN Issue_Code IS NULL THEN 0
        ELSE
        LENGTH(Issue_Code) - LENGTH(REGEXP_REPLACE(Issue_Code, ',', '')) + 1
    END
        ) AS Issues_Count,
        (CASE
            WHEN Clicks > 0 THEN TRUE
        ELSE
        FALSE
        END
        ) AS is_clicks,
        (CASE
            WHEN Conversions > 0 THEN TRUE
        ELSE
        FALSE
        END
        ) AS is_conversions,
        (CASE
            WHEN Impressions > 0 THEN TRUE
        ELSE
        FALSE
        END
        ) AS is_impressions,
        (CASE
            WHEN availability = 'in stock' THEN TRUE
        ELSE
        FALSE
        END
        ) AS in_stock,
        (CASE
            WHEN campaign_id IS NOT NULL THEN TRUE
        ELSE
        FALSE
        END
        ) AS is_targeted,
    FROM `{_PROJECT_NAME}.{dataset}.ACIT_Joined_Product_Data` as P 
    LEFT JOIN
    `{dataset}.ACIT_Joined_Ads_Data` as A 
    ON
    P.id = A.reference_prodId;
    """

  query_job = client.query(sql, job_config=job_config)
  query_job.result()

  print("Query Results loaded into table {}".format(table_id))


if __name__ == "__main__":
  bigquery_client = bigquery.Client()

  create_products_table(
      bigquery_client,
      constants.DATASET,
      constants.STORAGE_BUCKET,
      constants.TABLE_NAME_PRODUCTS,
  )
  create_product_status_table(
      bigquery_client,
      constants.DATASET,
      constants.STORAGE_BUCKET,
      constants.TABLE_NAME_PRODUCTSTATUS,
  )
  create_geo_table(
      bigquery_client,
      constants.DATASET,
      constants.STORAGE_BUCKET,
      constants.TABLE_NAME_GEO,
  )
  create_languages_table(
      bigquery_client,
      constants.DATASET,
      constants.STORAGE_BUCKET,
      constants.TABLE_NAME_LANGUAGES,
  )

  create_ads_table(
      bigquery_client,
      constants.DATASET,
      constants.STORAGE_BUCKET,
      constants.TABLE_NAME_ADS,
  )

  create_ads_final_table(bigquery_client, constants.PROJECT_NAME,
                         constants.DATASET)
  create_products_final_table(bigquery_client, constants.PROJECT_NAME,
                              constants.DATASET)

  create_final_table(bigquery_client, constants.PROJECT_NAME, constants.DATASET)
