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
"""Merchant Center *return policies* ingestion via Merchant API (v1)."""

from concurrent import futures
import json
from typing import Iterable, List, Tuple

from absl import logging
from acit import constants
from acit.api.v0.storage import schema_pb2
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.auth import credentials as _credentials
from google.protobuf import json_format
from google.shopping import merchant_accounts_v1 as ma


def _list_account_returns(
    client: ma.OnlineReturnPolicyServiceClient, account_id: str
) -> List[ma.OnlineReturnPolicy] | None:
  """Lists the v1 return policies for one account.

  Returns protobuf messages, not dicts; the caller wraps them in
  `schema_pb2.ReturnPolicies` on the way to disk.

  Args:
    client: The API client instance used to make the request.
    account_id: The ID of the account to query.

  Returns:
    A list of return policy messages -- empty if none -- or None if not
    accessible.
  """
  parent = f'accounts/{account_id}'
  try:
    pager = client.list_online_return_policies(
        request=ma.ListOnlineReturnPoliciesRequest(
            parent=parent, page_size=constants.PAGE_SIZE
        )
    )
    return list(pager)
  except gax_exceptions.PermissionDenied:
    logging.info(
        'Return policies not accessible for %s (PermissionDenied); skipping',
        account_id,
    )
    return None
  except gax_exceptions.NotFound:
    return []


def download_returns(
    credentials: _credentials.Credentials,
    account_ids: Iterable[str],
    mc_path: epath.Path,
    max_workers: int | None = None,
) -> None:
  """Downloads return policies from Merchant API v1, one file per account."""
  client = ma.OnlineReturnPolicyServiceClient(credentials=credentials)
  account_ids_list = list(account_ids)

  def _process(account_id: str) -> Tuple[str, int]:
    policies = _list_account_returns(client, account_id)
    if policies is None:
      return account_id, 0
    msg = schema_pb2.ReturnPolicies()
    msg.account_id = int(account_id)
    for p in policies:
      msg.returns.add().CopyFrom(ma.OnlineReturnPolicy.pb(p))

    record = json_format.MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    record[constants.METADATA_KEY] = {'accountId': account_id}

    output_file = (
        epath.Path(mc_path) / account_id / 'returns' / 'rows.jsonlines'
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')
    return account_id, len(policies)

  total = 0
  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {
        ex.submit(_process, aid): aid for aid in account_ids_list
    }
    for done in futures.as_completed(future_to_id):
      aid = future_to_id[done]
      try:
        account_id, n = done.result()
        total += n
        if n:
          logging.info('Wrote %d return policy(ies) for %s', n, account_id)
      except Exception as e:  # pylint: disable=broad-exception-caught
        logging.warning('Failed to process return policies for %s: %s', aid, e)

  if total == 0 and account_ids_list:
    account_id = account_ids_list[0]
    output_file = (
        epath.Path(mc_path) / account_id / 'returns' / 'rows.jsonlines'
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    msg = schema_pb2.ReturnPolicies()
    msg.account_id = int(account_id)
    record = json_format.MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    record[constants.METADATA_KEY] = {'accountId': account_id}
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')

  logging.info(
      'Merchant API returns: %d account(s) queried, %d policy(ies) total',
      len(account_ids_list),
      total,
  )
