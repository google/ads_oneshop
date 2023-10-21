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
"""Tools for identifying Product relations to Performance Max campaigns."""

# Omitting type hints for Google Ads, as this causes severe memory bloat,
#   is unavailable internally, and we're using JSON-parsed objects here.


from typing import Dict, Callable, List, Iterable, TypedDict, Any, Tuple

from acit import product


# 1-indexed dimension levels
_PRODUCT_DIMENSION_LEVELS = [
    'LEVEL1',
    'LEVEL2',
    'LEVEL3',
    'LEVEL4',
    'LEVEL5',
]

# 0-indexed dimension indices
_PRODUCT_DIMENSION_INDICES = [
    'INDEX0',
    'INDEX1',
    'INDEX2',
    'INDEX3',
    'INDEX4',
]

# A JSON object, specifically the ListingGroupFilterDimension from the Ads API
ListingGroupDimension = Any


class ListingGroupLeaf(TypedDict):
  dimension: ListingGroupDimension
  # JSON field format
  isTargeted: bool


class ListingGroupNode(TypedDict):
  dimension: ListingGroupDimension
  children: List['ListingGroupNode | ListingGroupLeaf']


ListingGroupTree = ListingGroupLeaf | ListingGroupNode


class TargetingMatcher:
  """Identify whether a given PMax Targeting tree matches a given product."""

  def __init__(self, category_cache: product.ProductCategoryCache):
    self._cache = category_cache

  def build_tree(
      self,
      path: List[ListingGroupDimension],
      tree: ListingGroupTree,
      is_targeted: bool,
  ):
    """Accumulates the input path recursively into a tree datastructure.

    Args:
      path: The API-provided ListingGroupFilterDimension[] path to a Listing Group leaf.
      tree: The accumulating tree.
      is_targeted: Whether the leaf at the end of this path is positively targeted.
    """
    # Negation of the base recursion case
    # We could return early, but beam complains about mixing yield and return
    if path:
      # Check if there's an existing child (this path shares common ancestor(s))
      assert 'children' in tree
      children: List[ListingGroupTree] = [
          c for c in tree['children'] if c['dimension'] == path[0]
      ]
      if children:
        self.build_tree(path[1:], children[0], is_targeted)
      else:
        # Is this a leaf?
        if len(path) == 1:
          leaf: ListingGroupLeaf = {
              'dimension': path[0],
              'isTargeted': is_targeted,
          }
          tree['children'].append(leaf)
        else:
          node: ListingGroupNode = {'dimension': path[0], 'children': []}
          tree['children'].append(node)
          self.build_tree(path[1:], node, is_targeted)

  def build_listing_group_tree(self, filters: List[Any]) -> ListingGroupTree:
    """Creates PMax targeting tree from Asset Group Listing Group Filters.

    Recursive. Google Ads limits depth to a single-digit number.

    Args:
      filters: The Google Ads Row object containing the Asset Group Listing Filter

    Returns:
      An iterable containing the generated tree, as required by the Beam API.
    """

    tree: ListingGroupNode = {'children': [], 'dimension': {}}

    assert filters
    for row in filters:
      asset_group_filter = row['assetGroupListingGroupFilter']
      path = asset_group_filter['path'].get('dimensions', [])
      is_targeted = 'UNIT_INCLUDED' == asset_group_filter['type']
      if not path:  # Only possible if this is a catch-all
        return {'isTargeted': is_targeted, 'dimension': {}}
      self.build_tree(path, tree, is_targeted)

    return tree

  def dimension_is_wildcard(self, dimension: ListingGroupDimension) -> bool:
    """Whether the input filter dimension is a wildcard (has no value)."""
    if 'productBiddingCategory' in dimension:
      return 'id' not in dimension['productBiddingCategory']
    if 'productBrand' in dimension:
      return 'value' not in dimension['productBrand']
    if 'productChannel' in dimension:
      return 'channel' not in dimension['productChannel']
    if 'productCondition' in dimension:
      return 'condition' not in dimension['productCondition']
    if 'productCustomAttribute' in dimension:
      return 'value' not in dimension['productCustomAttribute']
    if 'productItemId' in dimension:
      return 'value' not in dimension['productItemId']
    if 'productType' in dimension:
      return 'value' not in dimension['productType']
    return False

  def taxonomy_matches_dimension(
      self,
      product_taxonomy: str,
      dimension: ListingGroupDimension,
      dimension_key: str,
      depth_key: str,
      depth_names: List[str],
      test: Callable[[Any, str], bool],
  ) -> bool:
    """Whether the given taxonomy string matches the input dimension.

    Taxonomy strings are used for Google Product Categories and user-provided types.
    They are of the form "A > B > C".

    This method looks at the expected level of the dimension, and truncates (as
    necessary) the taxonomy string to only that many levels, useful when recursing
    through multiple dimension path levels.

    Args:
      product_taxonomy: The taxonomy string from the product.
      dimension: The ListingGroupDimension object to compare against.
      dimension_key: The `oneof` field name for this object.
      depth_key: The "level" key name within the object.
      depth_names: A lookup table for valid key values.
      test: A callable to test whether dimension[dimension_key] matches the
              depth-truncated taxonomy string.
    Returns:
      The result of the test
    """
    if dimension_key in dimension:
      info = dimension[dimension_key]
      depth = depth_names.index(info[depth_key])
      taxonomy_tokens = product_taxonomy.split(' > ')
      if depth >= len(taxonomy_tokens):
        # Ad criteria is too granular
        return False
      # We only want to match up to the depth specified in the dimension
      product_taxonomy = ' > '.join(taxonomy_tokens[: depth + 1])
      return test(info, product_taxonomy)
    return False

  def dimension_matches_product(
      self, product: Any, dimension: ListingGroupDimension
  ):
    """Whether the provided dimension matches the product."""
    if self.dimension_is_wildcard(dimension):
      return True

    if 'productBiddingCategory' in dimension:
      return self.taxonomy_matches_dimension(
          product.get('googleProductCategory', ''),
          dimension,
          'productBiddingCategory',
          'level',
          _PRODUCT_DIMENSION_LEVELS,
          lambda dimension, taxonomy: int(dimension['id'])
          == self._cache.id_by_path(taxonomy),
      )

    # All string comparisons should be case-insensitive
    # Ads and MC have inconsistent case processing.

    if 'productBrand' in dimension:
      return (
          dimension['productBrand']['value'].lower()
          == product.get('brand', '').lower()
      )
    if 'productChannel' in dimension:
      return (
          dimension['productChannel']['channel'].lower()
          == product.get('channel', '').lower()
      )
    if 'productCondition' in dimension:
      return (
          dimension['productCondition']['condition'].lower()
          == product.get('condition', '').lower()
      )
    if 'productCustomAttribute' in dimension:
      info = dimension['productCustomAttribute']
      depth = _PRODUCT_DIMENSION_INDICES.index(info['index'])
      product_label = product.get(f'customLabel{depth}', '')
      return info['value'].lower() == product_label.lower()
    if 'productItemId' in dimension:
      return (
          dimension['productItemId']['value'].lower()
          == product['offerId'].lower()
      )
    # TODO: Fix. Product Type is a freetext field within MC.
    #       There's an edge case where data is bad, and ">" does not
    #       adequately delimit. So "A > B > > > C" is [A, B, >, C].
    if 'productType' in dimension:
      return self.taxonomy_matches_dimension(
          # Always get the first one
          (product.get('productTypes', []) or [''])[0],
          dimension,
          'productType',
          'level',
          _PRODUCT_DIMENSION_LEVELS,
          lambda dimension, taxonomy: dimension['value'].lower()
          == taxonomy.split(' > ')[-1].lower()
          if taxonomy
          else False,
      )

    return False

  def product_targeted_by_tree(
      self, product: Any, tree: ListingGroupTree
  ) -> bool:
    """Recursively checks whether a product is targeted by a tree."""
    if 'isTargeted' in tree:
      return tree['isTargeted']

    assert 'children' in tree
    # Default to the first object (can update as necessary)
    catch_all = tree['children'][0]
    real_match = None

    for child in tree['children']:
      if self.dimension_is_wildcard(child['dimension']):
        catch_all = child
      elif self.dimension_matches_product(product, child['dimension']):
        real_match = child

    subtree = real_match or catch_all
    assert subtree, 'Full listing group filter tree missing.'
    return self.product_targeted_by_tree(product, subtree)

  def get_pmax_targeting(
      self,
      product: Any,
      merchant_to_cids: Dict[str, List[str]],
      cid_to_pmax_trees: Dict[str, List[ListingGroupTree]],
  ) -> Iterable[Tuple[Any, List[ListingGroupTree]]]:
    """Determines whether a product is targeted by any visible PMax campaigns.

    Conforms to Beam ParDo function return requirements.

    Args:
      product: The product to compare against
      mechant_to_cids: A lookup table of Merchant Center leaf IDs to Ads Customer IDs
      cid_to_pmax_trees: A multimap of cids to the unique listing group tree for each
        PMax asset group.

    Returns:
      An iterable Tuple of the product and all trees that match it.
    """

    matched_trees = []
    for cid in merchant_to_cids[product['accountId']]:
      for tree in cid_to_pmax_trees.get(cid, []):
        if self.product_targeted_by_tree(product, tree):
          matched_trees.append(tree)

    return [(product, matched_trees)]
