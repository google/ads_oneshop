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
"""Get language constants for Google Ads."""

import csv
import argparse
import sys
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

_QUERY = """
SELECT 
  language_constant.code,
  language_constant.id,
  language_constant.name,
  language_constant.resource_name, 
  language_constant.targetable 
FROM 
  language_constant"""

_LANGUAGE_CODES_FILE = './acit/api_datasets/data/lang_data/language_codes.csv'


def main(client, languages_file: str):
  ga_service = client.get_service("GoogleAdsService")
  with open(languages_file, 'wt') as f:
    writer = csv.writer(f)
    writer.writerow([
        'Language_Code', 'Criterion_ID', 'name', 'resource_name', 'targetable'
    ])
    # Issues a search request using streaming.
    stream = ga_service.search_stream(query=_QUERY,
                                      customer_id=client.login_customer_id)

    for batch in stream:
      for row in batch.results:

        listOfDetails = [
            row.language_constant.code, row.language_constant.id,
            row.language_constant.name, row.language_constant.resource_name,
            row.language_constant.targetable
        ]

        writer.writerow(listOfDetails)


if __name__ == "__main__":
  googleads_client = GoogleAdsClient.load_from_storage(
      os.path.expanduser("~/google-ads.yaml"))

  parser = argparse.ArgumentParser(
      description="Get language constants for Google Ads.")

  args = parser.parse_args()

  try:
    main(googleads_client, languages_file=_LANGUAGE_CODES_FILE)
  except GoogleAdsException as ex:
    print(f'Request with ID "{ex.request_id}" failed with status '
          f'"{ex.error.code().name}" and includes the following errors:')
    for error in ex.failure.errors:
      print(f'\tError with message "{error.message}".')
      if error.location:
        for field_path_element in error.location.field_path_elements:
          print(f"\t\tOn field: {field_path_element.field_name}")
    sys.exit(1)
