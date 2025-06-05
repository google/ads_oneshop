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

FROM apache/beam_python3.12_sdk:2.65.0

WORKDIR /src/

COPY . .

RUN \
  pip install --require-hashes -r requirements.txt \
  && pip install --no-deps --no-index dist/gtech_oneshop-*-py3-*-*.whl
