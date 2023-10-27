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


from typing import Optional, Dict, Tuple, List, Callable, Any, TypedDict, Iterable

from importlib import resources

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

_PRODUCT_CATEGORY_FILE = 'taxonomy-with-ids.en-US.txt'

# We load this later
CACHE: Optional['ProductCategoryCache'] = None


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


CACHE = ProductCategoryCache()
CACHE.load()


class TargetedLanguage(TypedDict):
  language: str
  is_targeted: bool


class Campaign(TypedDict):
  customer_id: str
  campaign_id: str
  campaign_type: str
  merchant_id: str
  sales_country: str
  feed_label: str
  enable_local: bool
  languages: list[TargetedLanguage]
  inventory_filter_dimensions: list[Any]


def build_campaign(
    campaign,
    languages: list[TargetedLanguage],
    inventory_filter_dimensions: list[Any],
) -> Iterable[Campaign]:
  shopping_settings = campaign['campaign'].get('shoppingSetting')
  if shopping_settings:
    return [
        Campaign(
            customer_id=campaign['customer']['id'],
            campaign_id=campaign['campaign']['id'],
            campaign_type=campaign['campaign']['advertisingChannelType'],
            merchant_id=shopping_settings['merchantId'],
            sales_country=shopping_settings.get('salesCountry', ''),
            feed_label=shopping_settings.get('feedLabel', ''),
            enable_local=shopping_settings.get('enableLocal', False),
            languages=languages,
            inventory_filter_dimensions=inventory_filter_dimensions,
        )
    ]
  return []


def set_product_in_stock(product):
  """Sets a key on the composite product status for product availability."""
  product['inStock'] = 'in stock' == product['product']['availability']
  return product


def set_product_approved(product):
  """Sets a key depending on whether the offer is approved in all locations."""
  # NOTE: Everything that can be targeted is always 'Shopping"
  destinations = [
      d
      for d in product['status']['destinationStatuses']
      if d['destination'] == 'Shopping'
  ]
  # Should only be one
  for destination in destinations:
    # Must not modify Beam inputs, so make a copy
    result = {**product}
    for k in ('approvedCountries', 'pendingCountries', 'disapprovedCountries'):
      result[k] = destination.get(k, [])
    return result
  return product


def taxonomy_matches_dimension(
    product_taxonomy: str,
    dimension: Any,
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


assert CACHE

_WILDCARD_DIMENSION_PATHS = {
    'productBiddingCategory': 'id',
    'productBrand': 'value',
    'productChannel': 'channel',
    'productChannelExclusivity': 'channelExclusivity',
    'productCondition': 'condition',
    'productCustomAttribute': 'value',
    'productItemId': 'value',
    'productType': 'value',
}


def dimension_is_wildcard(dimension) -> bool:
  for key, sub_key in _WILDCARD_DIMENSION_PATHS.items():
    if key in dimension:
      return sub_key not in dimension[key]
  return False


def dimension_matches_product(
    product: Any, dimension: Any, category_names_by_id: dict[str, str]
):
  """Whether the provided dimension matches the product."""
  if dimension_is_wildcard(dimension):
    return True

  if 'productBiddingCategory' in dimension:
    level = dimension['productBiddingCategory']['level']
    id_ = dimension['productBiddingCategory']['id']

    taxonomy_index = _PRODUCT_DIMENSION_LEVELS.index(level)
    taxonomy_tokens = product.get('googleProductCategory', '').split(' > ')
    if not taxonomy_index < len(taxonomy_tokens):
      # Ad criteria is too granular
      return False
    category_to_match = taxonomy_tokens[taxonomy_index]
    category = category_names_by_id[id_]
    return category_to_match == category

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
  # TODO: Channel exclusivity requires checking whether two offers IDs
  #       within the same feed label and language have only online or
  #       offline offers. This needs to be injected further up.
  #       For now (the setting seems rare), assume multichannel.
  if 'productChannelExclusivity' in dimension:
    return (
        dimension['productChannelExclusivity']['channelExclusivity'].lower()
        == product.get('channelExclusivity', 'MULTI_CHANNEL').lower()
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
    return taxonomy_matches_dimension(
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


class ProductTargetingNode(TypedDict):
  """Recursive Node for Product Targeting.

  Attributes:
    children: Any child nodes
    dimension: Ads targeting info. Empty on the root.
    is_targeted: Whether this branch is targeted. Leaf only.
  """

  children: List['ProductTargetingNode']
  dimension: Any
  # JSON field format
  isTargeted: Optional[bool]


class ProductTargetingTree(TypedDict):
  """Represents Ads targeting for a Product.

  Attributes:
    customer_id: The Customer ID
    campaign_id: The Campaign ID
    tree_parent_id: For Shopping Campaigns, the ad_group_id; for Performance Max, the asset_group_id.
  """

  customer_id: str
  campaign_id: str
  tree_parent_id: str
  node: ProductTargetingNode


def product_targeted_by_tree(
    product, node: ProductTargetingNode, category_names_by_id: dict[str, str]
) -> bool:
  """Recursively checks whether a product is targeted by a tree."""
  is_targeted = node.get('isTargeted')
  if is_targeted is not None:
    return is_targeted
  matcher = None
  wildcard = None
  for child in node['children']:
    dimension = child['dimension']
    if not dimension:
      # TODO: The Ads API does not support Collections.
      #       Since dimension can only be empty for the root node, if we
      #       hit here, we've encountered a second empty dimension. So,
      #       we assume conservatively that this product is not targeted.
      #       Besides, collections have fundamental differences with
      #       normal PMax shopping campaigns.
      return False
    if dimension_is_wildcard(dimension):
      wildcard = child
    elif dimension_matches_product(product, dimension, category_names_by_id):
      matcher = child
  first_match = matcher or wildcard
  assert first_match
  return product_targeted_by_tree(product, first_match, category_names_by_id)


def build_product_group_tree(
    dimensions: List[Any], node: ProductTargetingNode, is_targeted: bool
):
  """Recusively builds a ProductTargeting Tree.

  A Product Targeting tree is either a Listing Group for shopping ads,
  or an Asset Group Listing Group Filter for Performance Max.

  Presumes an empty top-level node at root.

  Args:
    dimensions: The array of dimensions from the campaign resource.
    node: The node of the tree to populate.
    is_targeted: Whether the leaf we're building to should be targeted.
  """

  if not dimensions:
    return
  assert node.get('isTargeted') is None
  head, rest = dimensions[0], dimensions[1:]
  # Does the child exist at this level?
  matches = [c for c in node['children'] if c['dimension'] == head]
  if matches:
    child = matches[0]
  else:
    child = ProductTargetingNode(
        children=[], dimension=head, isTargeted=None if rest else is_targeted
    )
    node['children'].append(child)
  build_product_group_tree(rest, child, is_targeted)


def campaign_matches_product_status(
    campaign: Campaign,
    product_status,
    category_names_by_id,
    language_codes_by_resource_name,
) -> bool:
  # TODO: unit test this
  product = product_status['product']
  if campaign['merchant_id'] != product_status['accountId']:
    return False
  campaign_label = (campaign['sales_country'] or campaign['feed_label']).lower()
  if campaign_label and campaign_label != product['feedLabel'].lower():
    return False
  if not campaign['enable_local'] and product['channel'] == 'local':
    return False
  for dimension in campaign['inventory_filter_dimensions']:
    if not dimension_matches_product(product, dimension, category_names_by_id):
      return False

  # Language targeting
  positive_languages = [
      language_codes_by_resource_name[lang['language']]
      for lang in campaign['languages']
      if lang['is_targeted']
  ]
  negative_languages = [
      language_codes_by_resource_name[lang['language']]
      for lang in campaign['languages']
      if not lang['is_targeted']
  ]

  if (
      positive_languages
      and product['contentLanguage'] not in positive_languages
  ):
    return False
  if product['contentLanguage'] in negative_languages:
    return False
  return True


def get_campaign_targeting(
    product: Any,
    trees_by_campaign_id: dict[str, list[ProductTargetingTree]],
    campaigns_by_merchant_id: dict[str, list[Campaign]],
    category_names_by_id: dict[str, str],
    language_codes_by_resource_name: dict[str, str],
) -> Iterable[Tuple[Any, list[ProductTargetingTree]]]:
  """Determines whether a product is targeted by any visible campaigns.

  Conforms to Beam ParDo function return requirements.

  Args:
    product: The product to compare against
    mechant_to_cids: A lookup table of Merchant Center leaf IDs to Ads Customer IDs
    cid_to_ad_group_trees: A multimap of cids to the unique listing group tree for each
      campaign. Works for PMax asset groups as well as Shopping ad groups.

  Returns:
    An iterable Tuple of the product and all trees that match it.
  """

  matched_trees = []
  for campaign in campaigns_by_merchant_id.get(product['accountId'], []):
    if not campaign_matches_product_status(
        campaign, product, category_names_by_id, language_codes_by_resource_name
    ):
      continue
    for tree in trees_by_campaign_id[campaign['campaign_id']]:
      if product_targeted_by_tree(
          product['product'], tree['node'], category_names_by_id
      ):
        matched_trees.append(tree)

  return [(product, matched_trees)]
