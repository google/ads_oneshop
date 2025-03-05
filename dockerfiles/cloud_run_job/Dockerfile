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

# Uses Python 3.11
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:500.0.0-stable

# gcloud image doesn't include certs or envsubst
RUN \
  apt-get update \
  && apt-get install -y ca-certificates gettext-base \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Make Python available on the path
ENV PATH="/usr/lib/google-cloud-sdk/platform/bundledpythonunix/bin:${PATH}"

WORKDIR /src/

COPY . .

RUN \
  pip install --require-hashes -r requirements.txt \
  && pip install --no-deps --no-index dist/ads_oneshop-*-py3-none-any.whl

CMD [ "src/acit/run_acit.sh" ]
