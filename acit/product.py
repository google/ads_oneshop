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
"""Merchant Center product data helpers."""


import itertools
from typing import Optional, Dict, Tuple

from importlib import resources

_PRODUCT_CATEGORY_FILE = 'taxonomy-with-ids.en-US.txt'


class ProductCategoryCache:
  """Caches Google Ads product category path lookups."""

  def __init__(self):
    self._loaded = False
    self._id_cache: Dict[int, str] = {}
    self._path_cache: Dict[str, int] = {}

  def load(self) -> None:
    """Initializes the cache."""
    if not self._loaded:
      with resources.files(__name__.rsplit('.', 1)[0]).joinpath(
          _PRODUCT_CATEGORY_FILE
      ).open('r') as f:
        for line in f:
          if line.startswith('#'):
            continue
          id_, path = line.strip().split(' - ')
          id_ = int(id_)
          self._id_cache[id_] = path
          self._path_cache[path] = id_
    self._loaded = True

  def path_by_id(self, id_: int) -> Optional[str]:
    """Gets the path for this leaf ID, or None."""
    self.load()
    return self._id_cache.get(id_)

  def id_by_path(self, path: str) -> Optional[int]:
    """Gets the ID by the path.

    Args:
      path: The full path of the Google Product taxonomy.

    Returns:
      The ID of the leaf in this path.
    """
    self.load()
    return self._path_cache.get(path)


def set_product_in_stock(product):
  """Sets a key on the composite product status for product availability."""
  product['inStock'] = 'in stock' == product['product']['availability']
  return product


def set_product_approved(product):
  """Sets a key depending on whether the offer is approved in all locations."""
  statuses = [s['status'] for s in product['status']['destinationStatuses']]
  approved = 'approved' in statuses
  pending = 'pending' in statuses
  disapproved = 'disapproved' in statuses
  # If any limitation appears, flag it
  # TODO: explicitly break out by property, since disapprovals vary
  product['approved'] = approved and not pending and not disapproved
  return product


def inner_join_product_and_status(k: Tuple[str, str], v):
  """Post-CoGroupByKey inner-join for 1:1 products and statuses.

  Used via beam.FlatMapTuple()

  Args:
    k: The composite merchant, offer key from the CoGroupByKey stage.
    v: The products and statuses associated with this key.

  Yields:
    A composite product status object with IDs lifted up.
  """
  merchant_id, offer_id = k
  # This should always be 1:1
  for t in itertools.product(v['products'], v['statuses']):
    yield {
        'accountId': merchant_id,
        'offerId': offer_id,
        'product': t[0],
        'status': t[1],
    }
