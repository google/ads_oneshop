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

if [[ -z "${DATAFLOW_REGION}" ]]; then
  echo '$DATAFLOW_REGION is required when using building dataflow images.' 1>&2
  exit 1
fi

if [[ -z "${GOOGLE_CLOUD_PROJECT}" ]]; then
  echo '$GOOGLE_CLOUD_PROJECT is required when using building dataflow images.' 1>&2
  exit 1
fi

if [[ -z "${IMAGES_REPO}" ]]; then
  echo '$IMAGES_REPO is required when using building dataflow images.' 1>&2
  exit 1
fi

if [[ -z "${CLOUD_BUILD_LOGS_URL}" ]]; then
  echo '$CLOUD_BUILD_LOGS_URL is required when using building dataflow images.' 1>&2
  exit 1
fi

if [[ -z "${CLOUD_BUILD_SERVICE_ACCOUNT}" ]]; then
  echo '$CLOUD_BUILD_SERVICE_ACCOUNT is required when using building dataflow images.' 1>&2
  exit 1
fi


export BUILD_TAG_BASE="${DATAFLOW_REGION}-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/${IMAGES_REPO}"

function build_images() {
        rm -f image_sources.tar image_sources.tar.gz
        tar cvf image_sources.tar --exclude-vcs --exclude-vcs-ignores .

        # TODO: Parameterize Python and Beam SDK version (currently 3.11/2.60.0)
        # TODO: Build the bdist/wheel once in a base container

        # dataflow worker image
        cp {,dataflow_}image_sources.tar
        tar rf dataflow_image_sources.tar -C dockerfiles/dataflow/ Dockerfile
        gzip -f dataflow_image_sources.tar
        gcloud builds submit \
          --gcs-log-dir="${CLOUD_BUILD_LOGS_URL}" \
          --service-account="${CLOUD_BUILD_SERVICE_ACCOUNT}" \
          --region="${DATAFLOW_REGION}" \
          --tag "${BUILD_TAG_BASE}/dataflow:latest" \
          dataflow_image_sources.tar.gz

        # cloud run job image
        cp {,cloud_run_job_}image_sources.tar
        tar rf cloud_run_job_image_sources.tar -C dockerfiles/cloud_run_job/ Dockerfile
        gzip -f cloud_run_job_image_sources.tar
        gcloud builds submit \
          --gcs-log-dir="${CLOUD_BUILD_LOGS_URL}" \
          --service-account="${CLOUD_BUILD_SERVICE_ACCOUNT}" \
          --region="${DATAFLOW_REGION}" \
          --tag "${BUILD_TAG_BASE}/cloud_run_job:latest" \
          cloud_run_job_image_sources.tar.gz

        rm *.tar*
}

build_images
