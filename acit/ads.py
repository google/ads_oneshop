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

from typing import cast, Iterable
from google.ads.googleads.v17.services.types import google_ads_service
from google.ads.googleads.v17.services.services.google_ads_service import client as service
from google.ads.googleads import client


def query(
    customer_id: str, query: str, ads_client: client.GoogleAdsClient
) -> Iterable[google_ads_service.GoogleAdsRow]:
  """
  Runs a query against an MCC structure

  Args:
    query: The GAQL query to run.

  Yields:
    Result GoogleAdsRow protos
  """

  stub = cast(
      service.GoogleAdsServiceClient,
      ads_client.get_service('GoogleAdsService'),
  )
  req = google_ads_service.SearchGoogleAdsStreamRequest(
      customer_id=customer_id, query=query
  )

  responses = stub.search_stream(request=req)

  for resp in responses:
    yield from resp.results or []
