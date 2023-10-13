# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Writes performance data for a given Customer ID to an output file."""

import csv
import sys

from google.ads.googleads.errors import GoogleAdsException

_QUERY = """
SELECT
  customer.id,
  campaign.id,
  segments.product_item_id,
  metrics.clicks,
  metrics.conversions,
  metrics.conversions_value,
  metrics.impressions,
  segments.product_aggregator_id,
  segments.product_bidding_category_level1,
  segments.product_bidding_category_level2,
  segments.product_bidding_category_level3,
  segments.product_bidding_category_level4,
  segments.product_bidding_category_level5,
  segments.product_brand,
  segments.product_country,
  segments.product_custom_attribute0,
  segments.product_custom_attribute1,
  segments.product_custom_attribute2,
  segments.product_custom_attribute3,
  segments.product_custom_attribute4,
  segments.product_merchant_id,
  segments.product_store_id,
  segments.product_type_l1,
  segments.product_type_l2,
  segments.product_type_l3,
  segments.product_type_l4,
  segments.product_type_l5,
  campaign.name,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  campaign.status,
  segments.date
FROM shopping_performance_view
WHERE
  campaign.advertising_channel_type IN ('LOCAL', 'LOCAL_SERVICES', 'PERFORMANCE_MAX', 'SHOPPING', 'SMART')
  AND segments.date DURING LAST_30_DAYS 
"""


def write(client, customer_id, login_customer_id, impressions_file) -> None:
  try:
    ga_service = client.get_service("GoogleAdsService")
    with open(impressions_file, "a") as f:
      writer = csv.writer(f)
      # Issues a search request using streaming.
      print(f"Processing Google Ads data for Customer ID {customer_id}...")
      stream = ga_service.search_stream(customer_id=customer_id, query=_QUERY)

      for batch in stream:
        for row in batch.results:
          listOfDetails = [
              login_customer_id,
              f"{row.customer.id}",
              f"{row.campaign.id}",
              f"{row.segments.product_item_id}",
              f"{row.metrics.clicks}",
              f"{row.metrics.conversions}",
              f"{row.metrics.conversions_value}",
              f"{row.metrics.impressions}",
              f"{row.segments.date}",
              f"{row.segments.product_aggregator_id}",
              f"{row.segments.product_bidding_category_level1}",
              f"{row.segments.product_bidding_category_level2}",
              f"{row.segments.product_bidding_category_level3}",
              f"{row.segments.product_bidding_category_level4}",
              f"{row.segments.product_bidding_category_level5}",
              f"{row.segments.product_brand}",
              f"{row.segments.product_country}",
              f"{row.segments.product_custom_attribute0}",
              f"{row.segments.product_custom_attribute1}",
              f"{row.segments.product_custom_attribute2}",
              f"{row.segments.product_custom_attribute3}",
              f"{row.segments.product_custom_attribute4}",
              f"{row.segments.product_merchant_id}",
              f"{row.segments.product_store_id}",
              f"{row.segments.product_type_l1}",
              f"{row.segments.product_type_l2}",
              f"{row.segments.product_type_l3}",
              f"{row.segments.product_type_l4}",
              f"{row.segments.product_type_l5}",
              f"{row.campaign.name}",
              f"{row.campaign.advertising_channel_type}",
              f"{row.campaign.advertising_channel_sub_type}",
              f"{row.campaign.status}",
              f"{row.segments.product_channel}",
              f"{row.segments.product_country}",
              f"{row.segments.product_language}",
          ]

          writer.writerow(listOfDetails)

  except GoogleAdsException as ex:
    print(
        f'Request with ID "{ex.request_id}" failed with status '
        f'"{ex.error.code().name}" and includes the following errors:'
    )
    for error in ex.failure.errors:
      print(f'\tError with message "{error.message}".')
      if error.location:
        for field_path_element in error.location.field_path_elements:
          print(f"\t\tOn field: {field_path_element.field_name}")
    sys.exit(1)
