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

COPY --from=golang:1.23 /usr/local/go/ /usr/local/go/

ENV PATH="/usr/local/go/bin:${PATH}"

RUN pip install -U pip wheel build

WORKDIR /src/

COPY . .

# Install must be editable.
# Both unittest discovery, as well as the generated schema artifacts
# require this. By contrast, bazel is simpler to reason about.
RUN python -m pip install -e .

CMD [ "acit/run_acit.sh" ]
