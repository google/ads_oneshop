#!/bin/bash
# Copyright 2024 Google LLC
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

set -e
set -x

if [[ -z "${DATAFLOW_REGION}" ]]; then
  echo '$DATAFLOW_REGION is required.' 1>&2
  exit 1
fi

# TODO: Pass parameter overrides here
# TODO: Put this config in terraform somehow and reuse
gcloud run jobs execute ads-oneshop-job --region "${DATAFLOW_REGION}" --wait
