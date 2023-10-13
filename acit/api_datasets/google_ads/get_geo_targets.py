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
"""Get geo constants for Google Ads."""

import csv
import argparse
import sys
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

_QUERY = """
SELECT 
  geo_target_constant.canonical_name, geo_target_constant.country_code, geo_target_constant.id, geo_target_constant.name, 
  geo_target_constant.parent_geo_target, geo_target_constant.resource_name, geo_target_constant.status, 
  geo_target_constant.target_type 
FROM 
  geo_target_constant
"""

_GEO_TARGETS_FILE = "./acit/api_datasets/data/geo_data/geo_targets.csv"


def main(client, geo_filename: str):
  ga_service = client.get_service("GoogleAdsService")

  with open(geo_filename, "wt") as f:
    writer = csv.writer(f)
    writer.writerow([
        "canonical_name",
        "Country_Code",
        "Criteria_ID",
        "name",
        "parent_geo_target",
        "resource_name",
        "status",
        "target_type",
    ])
    # Issues a search request using streaming.
    stream = ga_service.search_stream(
        query=_QUERY, customer_id=client.login_customer_id
    )

    for batch in stream:
      for row in batch.results:
        listOfDetails = [
            f"{row.geo_target_constant.canonical_name}",
            f"{row.geo_target_constant.country_code}",
            f"{row.geo_target_constant.id}",
            f"{row.geo_target_constant.name}",
            f"{row.geo_target_constant.parent_geo_target}",
            f"{row.geo_target_constant.resource_name}",
            f"{row.geo_target_constant.status}",
            f"{row.geo_target_constant.target_type}",
        ]

        writer.writerow(listOfDetails)


if __name__ == "__main__":
  # GoogleAdsClient will read the google-ads.yaml configuration file in the
  # home directory if none is specified.
  googleads_client = GoogleAdsClient.load_from_storage(
      os.path.expanduser("~/google-ads.yaml")
  )

  parser = argparse.ArgumentParser(
      description="Get geo constants for Google Ads"
  )

  args = parser.parse_args()

  try:
    main(googleads_client, geo_filename=_GEO_TARGETS_FILE)
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
