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
"""Download JSON resources from the Google discovery APIs."""

from googleapiclient import discovery

from absl import app
from absl import flags
from absl import logging

from typing import cast, Dict, Any, Iterable

import json
import sys
from google import auth

_API_NAME = flags.DEFINE_string('api_name', '', 'The discovery API name to use')

_API_VERSION = flags.DEFINE_string(
    'api_version', '', 'The discovery API version to use'
)

_RESOURCE_NAME = flags.DEFINE_string(
    'resource_name', '', 'The resource name to call'
)

_RESOURCE_METHOD = flags.DEFINE_string(
    'resource_method', 'list', 'The resource method to use.'
)

_METADATA = flags.DEFINE_string(
    'metadata',
    '',
    (
        'Metadata of the form "k1=v1;k2=v2". '
        'Injected into resources. '
        'Values can also be injected from parent '
        'resources via placeholders @field_name'
    ),
)

_PARAMS = flags.DEFINE_string(
    'params',
    '',
    (
        'API parameters of the form "k1=v1;k2=v2". '
        'Values can also be injected from parent '
        'resources via placeholders @field_name'
    ),
)

_PARENT_RESOURCE = flags.DEFINE_string(
    'parent_resource', '', 'The parent resource to query, if applicable'
)

_PARENT_PARAMS = flags.DEFINE_string(
    'parent_params',
    '',
    'The params to pass to the parent resource, if applicable',
)

_RESULT_PATH = flags.DEFINE_string(
    'result_path',
    'resources',
    'The path to the results in the response object.',
)

METADATA_KEY = 'downloaderMetadata'


def _substitute(params: Dict[str, Any], substitutions: Dict[str, Any]):
  copy: Dict[str, Any] = {}
  for k, v in params.items():
    # May be an object already if consumed from another library
    copy[k] = v
    if type(v) == str:
      # TODO: make this more intelligent i.e. in-JSON substitutions
      # May need to use another macro like @@
      if substitutions:
        if v.startswith('@'):
          copy[k] = substitutions.get(v.split('@')[1])
      if v.startswith('{'):
        copy[k] = json.loads(v)
  return copy


def get_results(
    collection: str,
    params: Dict[str, Any],
    resource_method: str,
    result_path: str,
    metadata: Dict[str, Any],
):
  request = getattr(collection(), resource_method)(**params)
  while request:
    response = request.execute()
    for result in response.get(result_path, []):
      result[METADATA_KEY] = metadata or {}
      yield result
    request = getattr(collection(), f'{resource_method}_next')(
        request, response
    )


def parse_params(param_input):
  params = {}
  for p in cast(str, param_input).split(';'):
    if p:
      k, v = p.split('=', maxsplit=1)
      params.update({k: v})
  return params


def download_resources(
    client,
    resource_name: str,
    params: Dict[str, Any],
    parent_resource: str,
    parent_params: Dict[str, Any],
    resource_method: list,
    result_path: str,
    metadata: Dict[str, Any],
) -> Iterable[Any]:
  collection = getattr(client, resource_name)
  if parent_resource:
    parent_collection = getattr(client, parent_resource)
    parents = get_results(
        parent_collection, parent_params, 'list', 'resources', metadata
    )
    for parent in parents:
      substituted_params = _substitute(params, parent)
      substituted_metadata = _substitute(metadata, parent)
      yield from get_results(
          collection,
          substituted_params,
          resource_method,
          result_path,
          substituted_metadata,
      )
  else:
    yield from get_results(
        collection, params, resource_method, result_path, metadata
    )


def main(_):
  api_name = _API_NAME.value
  api_version = _API_VERSION.value
  resource_name = _RESOURCE_NAME.value
  resource_method = _RESOURCE_METHOD.value
  result_path = _RESULT_PATH.value

  if not api_name or not api_version or not resource_name:
    logging.error('API name, version, and resource are required.')
    sys.exit(1)

  creds, _ = auth.default()
  client = discovery.build(api_name, api_version, credentials=creds)
  params = parse_params(_PARAMS.value)
  parent_resource = _PARENT_RESOURCE.value
  parent_params = parse_params(_PARENT_PARAMS.value)
  metadata = parse_params(_METADATA.value)
  for result in download_resources(
      client,
      resource_name,
      params,
      parent_resource,
      parent_params,
      resource_method,
      result_path,
      metadata,
  ):
    print(json.dumps(result))


if __name__ == '__main__':
  app.run(main)
