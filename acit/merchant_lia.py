# Copyright 2026 Google LLC
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
"""Merchant Center omnichannel / LIA ingestion via the Merchant API (stable v1).

This replaces the old Content API
`liasettings.get` (per account) + `liasettings.list` (MCA roll-down) pulls
as part of the Content API -> Merchant API migration.

The Merchant API has no `liasettings` resource: the Local Inventory Ads /
local-storefront config now lives in **OmnichannelSettings**, listed per region
under each account via
``OmnichannelSettingsServiceClient.list_omnichannel_settings``. There is also no
MCA roll-down `list`, and the aggregator itself is NOT a valid parent for this
method (it returns ``PermissionDenied`` -- "only subaccounts and standalone
accounts"). So we pull per (sub)account directly and never query the aggregator.

Output is written in the NATIVE v1 shape
(snake_case keys, enum NAMES as strings) as one FLAT record per account --
``{"account_id": <int>, "omnichannel_settings": [<OmnichannelSetting>, ...]}``
-- to ``<mc_path>/<account_id>/liasettings/rows.jsonlines``,
preserving the existing BigQuery glob
(``merchant_center/*/liasettings/*.jsonlines``). The old
``{settings, children[]}`` envelope is gone; downstream SQL recovers the
MCA<->child relationship from the (already migrated) `accounts` table.
"""

from concurrent import futures
import json
from typing import Iterable, List, Tuple

from absl import logging
from acit import constants
from acit.api.v0.storage import schema_pb2
from acit.constants import METADATA_KEY
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.auth import credentials as _credentials
from google.protobuf import json_format
from google.shopping import merchant_accounts_v1 as ma


def _list_account_omnichannel_settings(
    client: ma.OmnichannelSettingsServiceClient, account_id: str
) -> List[ma.OmnichannelSetting] | None:
  """Lists the v1 omnichannel settings for one account.

  Returns protobuf messages, not dicts; the caller wraps them in
  `schema_pb2.OmnichannelLiaSettings` on the way to disk.

  Args:
    client: The API client instance used to make the request.
    account_id: The ID of the account to query.

  Returns:
    A list of per-region OmnichannelSetting messages -- empty if the account
    has none -- or None if the account is not a valid parent for this method
    (e.g. an aggregator or MCA, which returns PermissionDenied).
  """
  parent = f'accounts/{account_id}'
  try:
    pager = client.list_omnichannel_settings(
        request=ma.ListOmnichannelSettingsRequest(
            parent=parent, page_size=constants.PAGE_SIZE
        )
    )
    return list(pager)
  except gax_exceptions.PermissionDenied:
    # Aggregators/MCAs are not valid parents
    # "only subaccounts and standalone
    # accounts". The old Content API aggregator `get` returned empty anyway.
    logging.info(
        'Omnichannel settings not accessible for %s '
        '(not a sub-/standalone account); skipping', account_id)
    return None
  except gax_exceptions.NotFound:
    return []


def download_omnichannel_settings(
    credentials: _credentials.Credentials,
    account_ids: Iterable[str],
    mc_path: epath.Path,
    max_workers: int | None = None) -> None:
  """Downloads omnichannel/LIA settings from Merchant API v1.

  Writes output to one file per account.

  Args:
    credentials: Google credentials (same as used for the Ads/Content APIs).
    account_ids: iterable of sub-/standalone Merchant Center account
      IDs to pull. (Aggregators are skipped automatically-- they
      are not valid parents.)
    mc_path: epath.Path to the merchant_center output directory.
    max_workers: max concurrent account downloads. Callers (namely `acit.py`)
      should pass this explicitly so it stays consistent with the other
      Merchant API ingestion stages.
  """
  client = ma.OmnichannelSettingsServiceClient(credentials=credentials)
  account_ids_list = list(account_ids)

  def _process(account_id: str) -> Tuple[str, int]:
    settings = _list_account_omnichannel_settings(client, account_id)
    # None (not accessible) or [] (no settings) -> write nothing;
    # LEFT JOINs default such accounts to "not implemented".
    if not settings:
      return account_id, 0
    msg = schema_pb2.OmnichannelLiaSettings()
    msg.account_id = int(account_id)
    for s in settings:
      msg.omnichannel_settings.add().CopyFrom(ma.OmnichannelSetting.pb(s))

    record = json_format.MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    record[METADATA_KEY] = {'accountId': account_id}

    output_file = (epath.Path(mc_path) / account_id / 'liasettings' /
                   'rows.jsonlines')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')
    return account_id, len(settings)

  total = 0
  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {ex.submit(_process, aid): aid for aid in account_ids_list}
    for done in futures.as_completed(future_to_id):
      account_id, n = done.result()  # surface exceptions
      total += n
      if n:
        logging.info('Wrote %d omnichannel setting(s) for %s', n,
                     account_id)

  logging.info(
      'Merchant API omnichannel: %d account(s) queried, %d setting(s) total',
      len(account_ids_list), total)
