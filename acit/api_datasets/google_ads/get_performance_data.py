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
"""Gets performance data from all leaf accounts under a login Customer ID."""

import argparse
import sys
import os
from . import write_shopping_performance

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

_GAQL_LEAF_ACCOUNTS = """
  SELECT
    customer_client.id,
    customer_client.descriptive_name,
    customer_client.manager,
    customer.status
  FROM customer_client
  WHERE
    customer_client.status = 'ENABLED'
    AND customer_client.manager = false
"""


def main(client, login_customer_id=None):
  """Gets performance data from all leaf accounts under a login Customer ID.

  Args:
    client: The Google Ads client.
    login_customer_id: Optional manager account ID. If none provided, this
    method will instead list the accounts accessible from the
    authenticated Google Ads account.
  """

  googleads_service = client.get_service("GoogleAdsService")
  customer_service = client.get_service("CustomerService")

  seed_customer_ids = []

  if login_customer_id is not None:
    seed_customer_ids = [login_customer_id]
  else:
    print("No manager ID is specified. Using accessible customer IDs.")

    customer_resource_names = (
        customer_service.list_accessible_customers().resource_names
    )

    for customer_resource_name in customer_resource_names:
      customer_id = googleads_service.parse_customer_path(
          customer_resource_name
      )["customer_id"]
      seed_customer_ids.append(customer_id)

  performance_file = "./acit/api_datasets/data/ads_data/impressions.csv"
  for seed_customer_id in seed_customer_ids:
    response = googleads_service.search(
        customer_id=str(seed_customer_id), query=_GAQL_LEAF_ACCOUNTS
    )
    for googleads_row in response:
      customer_client = googleads_row.customer_client
      customer_id = customer_client.id
      write_shopping_performance.write(
          client, str(customer_id), login_customer_id, performance_file
      )


if __name__ == "__main__":
  # GoogleAdsClient will read the google-ads.yaml configuration file in the
  # home directory if none is specified.
  googleads_client = GoogleAdsClient.load_from_storage(
      os.path.expanduser("~/google-ads.yaml")
  )

  parser = argparse.ArgumentParser(
      description="This example gets the account hierarchy of the specified "
      "manager account and login customer ID."
  )
  # The following argument(s) should be provided to run the example.
  parser.add_argument(
      "-l",
      "--login_customer_id",
      "--manager_customer_id",
      type=str,
      required=False,
      help="Optional manager "
      "account ID. If none provided, the example will "
      "instead list the accounts accessible from the "
      "authenticated Google Ads account.",
  )
  args = parser.parse_args()
  googleads_client.login_customer_id = args.login_customer_id
  try:
    main(googleads_client, args.login_customer_id)
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
