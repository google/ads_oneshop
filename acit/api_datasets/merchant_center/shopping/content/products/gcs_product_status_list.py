# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Gets all product statuses on the specified account."""

import sys
import json

from . import common

# The maximum number of results to be returned in a page.
MAX_PAGE_SIZE = 50

_PRODUCT_STATUS_FILE = (
    './acit/api_datasets/data/products_data/productstatus.json'
)


def main(argv):
  # Authenticate and construct service.
  service, config, _ = common.init(argv, __doc__)
  merchant_id = config['merchantId']
  common.check_mca(config, True)

  account_request = service.accounts().list(
      merchantId=merchant_id, maxResults=MAX_PAGE_SIZE
  )

  with open(_PRODUCT_STATUS_FILE, 'wt') as f:
    while account_request is not None:
      result = account_request.execute()
      accounts = result.get('resources')
      if not accounts:
        print(f'No child accounts were found for account {merchant_id}')
        break
      for account in accounts:
        account_id = str(account['id'])
        print(account_id)
        product_status_request = service.productstatuses().list(
            merchantId=account_id
        )
        while product_status_request is not None:
          result2 = product_status_request.execute()
          products = result2.get('resources')
          if not products:
            print(f'No products were found for account {account_id}')
            break
          json_object = json.dumps(products)
          f.write(json_object)
          product_status_request = service.productstatuses().list_next(
              product_status_request, result2
          )

      account_request = service.accounts().list_next(account_request, result)


if __name__ == '__main__':
  main(sys.argv)
