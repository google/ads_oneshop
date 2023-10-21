# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helpers for account-level Merchant Center data."""

from typing import Generator, Tuple, Any


def extract_ads_ids_by_merchant(
    account: Any,
) -> Generator[Tuple[str, str], None, None]:
  """Maps each leaf Merchant Center account to all associated Ads accounts.

  Includes Ads accounts associated at the MCA level.

  Output conforms to the expectations for a functional ParDo passed to beam.FlatMap().

  Args:
    account: The account object returned by the API, enhanced with a 'children'
               key if an MCA.

  Yields:
    A tuple of the leaf Merchant account ID and the Ads customer ID.
  """
  account_id = account['id']
  top_level_customer_ids = set(
      [
          a['adsId']
          for a in account.get('adsLinks', [])
          if a['status'] == 'active'
      ]
  )
  if 'children' in account:
    for child in account['children']:
      merchant_id = child['id']
      child_customer_ids = set(
          [
              a['adsId']
              for a in child.get('adsLinks', [])
              if a['status'] == 'active'
          ]
      )
      # Don't double-count a customer ID
      all_cids = child_customer_ids | top_level_customer_ids
      # 0-valued customer IDs need to be highlighted for targeting
      if not all_cids:
        yield merchant_id, '0'
      else:
        for customer_id in all_cids:
          # TODO: it may make sense to roll-down MCA-level info here into the child
          # Rename this method if so.
          yield merchant_id, customer_id
  else:
    if not top_level_customer_ids:
      yield account_id, '0'
    else:
      for customer_id in top_level_customer_ids:
        yield account_id, customer_id
