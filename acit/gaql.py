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
"""Runs a Google Ads Query Language (GAQL) query across an MCC tree.

Writes GoogleAdsRow protos in sparse jsonlines format to the specified output
directory. Filenames contain an optional query prefix to aid in concatenation;
the execution time of the query to allow easy sorting; and the Customer ID of
the account the query ran against.
"""

from concurrent import futures
import datetime
import enum
import multiprocessing as mp
import textwrap
from typing import Dict

from absl import app
from absl import flags
from absl import logging

from acit import ads

from etils import epath

from google.ads.googleads import client
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import json_format

_LOGIN_CUSTOMER_ID = flags.DEFINE_string(
    'login_customer_id',
    None,
    'The login customer ID to authenticate against',
)

_ROOT_CUSTOMER_ID = flags.DEFINE_string(
    'root_customer_id',
    '',
    (
        'The root customer ID to query accounts below. '
        'If not specified, use the login customer ID.'
    ),
)

_QUERY = flags.DEFINE_string(
    'query',
    '',
    (
        'The GAQL query to run against all accounts. '
        'If not specified, returns all campaigns.'
    ),
)

_OUTPUT_DIRECTORY = epath.DEFINE_path(
    'output_dir', '', 'The directory to output leaf query results.'
)

_PREFIX = flags.DEFINE_string(
    'prefix',
    'google-ads-gaql',
    'The prefix to add to each query file. Useful for selecting files.',
)

_ADS_API_VERSION = flags.DEFINE_string(
    'ads_api_version',
    'v14',
    'The version of the Ads API to use',
)


class QueryMode(enum.Enum):
  LEAVES = 'leaves'
  MCCS = 'mccs'
  ALL = 'all'
  SINGLE = 'single'


def get_children_query(mode: QueryMode = QueryMode.LEAVES):
  clause = 'AND customer_client.id'
  if mode == QueryMode.SINGLE:
    clause = 'AND customer_client.level = 0'
  if mode == QueryMode.LEAVES:
    clause = 'AND customer_client.manager = false'
  if mode == QueryMode.MCCS:
    clause = 'AND customer_client.manager = true'

  return textwrap.dedent(
      f"""\
  SELECT
    customer_client.id,
    customer_client.descriptive_name,
    customer_client.manager,
    customer.status
  FROM customer_client
  WHERE
    customer_client.status = 'ENABLED'
    {clause}"""
  )


GAQL_CAMPAIGNS = textwrap.dedent(
    """\
  SELECT
    customer.id,
    customer.descriptive_name,
    campaign.id,
    campaign.name,
    campaign.status
  FROM campaign"""
)


def get_directory_path(orig_path: str) -> epath.Path:
  path = epath.Path(orig_path)
  if not path.is_dir():
    raise ValueError(
        'Provided path does not exist or is not a folder: %s' % path
    )
  return path


def query_to_file(
    customer_id: str,
    query: str,
    ads_client: client.GoogleAdsClient,
    prefix: str = '',
    output_dir: str = '',
):
  # TODO: Add a job ID either before or after the timestamp.
  filename = (
      f'{prefix}'
      f'-{datetime.datetime.now(datetime.timezone.utc).isoformat()}'
      f'-{customer_id}.jsonlines'
  )
  full_dir = get_directory_path(output_dir)
  path = full_dir / filename
  with path.open(mode='w') as file:
    # TODO: Add retry logic or backoff for robustness
    for row in ads.query(
        customer_id=customer_id, query=query, ads_client=ads_client
    ):
      print(json_format.MessageToJson(row, indent=None), file=file)


def run_query(
    query: str,
    ads_client: client.GoogleAdsClient,
    customer_id: str,
    prefix: str = '',
    output_dir: str = '',
    query_mode: QueryMode = QueryMode.LEAVES,
    validate_only: bool = False,
) -> None:
  """Run a query against a Google Ads account tree and write jsonlines files.

  Due to limitations of the GIL and Python's gRPC implementation, this procedure
  creates an output file containing GoogleAdsRow JSON for each account queried,
  unless `validate_only` is set.

  Args:
    query: The GAQL query to run.
    ads_client: The Google Ads client to use.
    customer_id: The customer ID to run the query against.
    prefix: The filename prefix to add to each output file.
    output_dir: The directory to output the results.
    query_mode: The expansion behavior for this query.
    validate_only: Whether to validate the query only.

  Raises:
    GoogleAdsException: If this query results in an error using `validate_only`.
  """
  if validate_only:
    # TODO: change validation behavior depending on query mode.
    # Grab just the first child
    child = next(
        iter(
            ads.query(
                customer_id=customer_id,
                ads_client=ads_client,
                query=get_children_query(),
            )
        )
    )

    # Force an exception if a query fails
    _ = list(
        ads.query(
            customer_id=str(child.customer_client.id),
            ads_client=ads_client,
            query=query,
        )
    )
    return
  try:
    with futures.ProcessPoolExecutor(
        mp_context=mp.get_context('spawn')
    ) as executor:
      future_results: Dict[futures.Future[None], int] = {}
      for row in ads.query(
          customer_id=customer_id,
          ads_client=ads_client,
          query=get_children_query(query_mode),
      ):
        leaf_id = row.customer_client.id
        future_results.update(
            {
                executor.submit(
                    query_to_file,
                    customer_id=str(leaf_id),
                    query=query,
                    ads_client=ads_client,
                    prefix=prefix,
                    output_dir=output_dir,
                ): leaf_id
            }
        )

      for completed in futures.as_completed(future_results):
        # Raise an exception if one occurred
        completed.result()
  except Exception as executor_exception:
    logging.exception(executor_exception)


def main(unused_argv):
  # TODO: Parameterize the client constructor
  ads_client = client.GoogleAdsClient.load_from_storage(
      version=_ADS_API_VERSION.value
  )
  ads_client.login_customer_id = _LOGIN_CUSTOMER_ID.value
  query = _QUERY.value or GAQL_CAMPAIGNS
  run_query(
      query,
      ads_client,
      _ROOT_CUSTOMER_ID.value or _LOGIN_CUSTOMER_ID.value,
      prefix=_PREFIX.value,
      output_dir=str(_OUTPUT_DIRECTORY.value),
  )


if __name__ == '__main__':
  app.run(main)
