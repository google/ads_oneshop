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
"""Merchant Center *reports* ingestion via the Merchant API (stable v1)."""

from concurrent import futures
import json
from typing import Iterable, Tuple

from absl import logging
from acit import constants
from acit.api.v0.storage import schema_pb2
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.auth import credentials as _credentials
from google.protobuf import json_format
from google.shopping import merchant_reports_v1 as mr

_MARKET_INSIGHTS_QUERY = """
SELECT
  price_competitiveness_product_view.id,
  price_competitiveness_product_view.report_country_code
FROM price_competitiveness_product_view
LIMIT 1
"""

_PRODUCT_VIEW_ISSUES_QUERY = """
SELECT
  product_view.id,
  product_view.offer_id,
  product_view.item_issues
FROM product_view
"""


def _check_market_insights(client: mr.ReportServiceClient, parent: str) -> bool:
  """Checks if Market Insights is enabled by querying price competitiveness."""
  try:
    pager = client.search(
        request=mr.SearchRequest(
            parent=parent, query=_MARKET_INSIGHTS_QUERY, page_size=1
        )
    )
    # If the call succeeds and we can fetch the first page, it's enabled.
    rows = list(pager)
    return len(rows) > 0
  except gax_exceptions.PermissionDenied:
    logging.info(
        'Market Insights query returned PermissionDenied for %s', parent
    )
    return False
  except Exception as e:  # pylint: disable=broad-exception-caught
    logging.warning('Market Insights query failed for %s: %s', parent, e)
    return False


def _list_structured_data_issues(
    client: mr.ReportServiceClient, parent: str
) -> Iterable[mr.ProductView]:
  """Queries product_view to stream structured data issues lazily.

  Yields protobuf messages; the caller streams them to disk rather than
  materializing the entire account's catalog into memory.

  Args:
    client: The API client instance used to make the request.
    parent: The resource name of the account to query (e.g. 'accounts/123').

  Yields:
    ProductView messages.
  """
  try:
    pager = client.search(
        request=mr.SearchRequest(
            parent=parent,
            query=_PRODUCT_VIEW_ISSUES_QUERY,
            page_size=constants.PAGE_SIZE,
        )
    )
    for row in pager:
      yield row.product_view
  except gax_exceptions.PermissionDenied:
    logging.info(
        'ProductView query returned PermissionDenied for %s', parent
    )
    return
  except Exception as e:  # pylint: disable=broad-exception-caught
    logging.warning('ProductView query failed for %s: %s', parent, e)
    return


def download_reports(
    credentials: _credentials.Credentials,
    account_ids: Iterable[str],
    mc_path: epath.Path,
    max_workers: int | None = None,
) -> None:
  """Downloads reports diagnostics (Market Insights & Structured Data)."""
  client = mr.ReportServiceClient(credentials=credentials)
  account_ids_list = list(account_ids)

  def _process(account_id: str) -> Tuple[str, int]:
    parent = f'accounts/{account_id}'
    has_market_insights = _check_market_insights(client, parent)

    output_file = (
        epath.Path(mc_path) / account_id / 'reports' / 'rows.jsonlines'
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with output_file.open('w') as f:
      for pv in _list_structured_data_issues(client, parent):
        msg = schema_pb2.ReportsSettings()
        msg.account_id = int(account_id)
        msg.has_market_insights = has_market_insights
        pv_dict = json_format.MessageToDict(
            mr.ProductView.pb(pv),
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        diag = msg.structured_data_issues.add()
        json_format.ParseDict(pv_dict, diag, ignore_unknown_fields=True)

        record = json_format.MessageToDict(
            msg,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        record[constants.METADATA_KEY] = {'accountId': account_id}
        f.write(json.dumps(record) + '\n')
        count += 1

      if count == 0:
        msg = schema_pb2.ReportsSettings()
        msg.account_id = int(account_id)
        msg.has_market_insights = has_market_insights
        record = json_format.MessageToDict(
            msg,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        record[constants.METADATA_KEY] = {'accountId': account_id}
        f.write(json.dumps(record) + '\n')
        count = 1

    return account_id, count

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
          logging.info('Wrote %d reports record(s) for %s', n, account_id)
      except Exception as e:  # pylint: disable=broad-exception-caught
        logging.warning('Failed to process reports for %s: %s', aid, e)

  if total == 0 and account_ids_list:
    account_id = account_ids_list[0]
    output_file = (
        epath.Path(mc_path) / account_id / 'reports' / 'rows.jsonlines'
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    msg = schema_pb2.ReportsSettings()
    msg.account_id = int(account_id)
    msg.has_market_insights = False
    record = json_format.MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    record[constants.METADATA_KEY] = {'accountId': account_id}
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')

  logging.info(
      'Merchant API reports: %d account(s) queried, %d record(s) written',
      len(account_ids_list),
      total,
  )
