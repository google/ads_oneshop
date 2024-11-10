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

if [[ -z "${GOOGLE_CLOUD_PROJECT}" ]]; then
  echo '$GOOGLE_CLOUD_PROJECT is required.' 1>&2
  exit 1
fi

if [[ -z "${DATAFLOW_REGION}" ]]; then
  echo '$DATAFLOW_REGION is required.' 1>&2
  exit 1
fi

if [[ -z "${IMAGES_REPO}" ]]; then
  echo '$IMAGES_REPO is required.' 1>&2
  exit 1
fi

# TODO: Inject these directly from terraform
export CLOUD_RUN_SERVICE_ACCOUNT="oneshop-cloudrun-sa@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"
export DATAFLOW_SERVICE_ACCOUNT="oneshop-dataflow-sa@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"
export DATAFLOW_TEMP_BUCKET="gs://${GOOGLE_CLOUD_PROJECT}_oneshop_dataflow_temp/"
export DATAFLOW_STAGING_BUCKET="gs://${GOOGLE_CLOUD_PROJECT}_oneshop_dataflow_staging/"

if [[ -z "${CUSTOMER_IDS}" ]]; then
  echo '$CUSTOMER_IDS is required.' 1>&2
  exit 1
fi

if [[ -z "${MERCHANT_IDS}" ]]; then
  echo '$MERCHANT_IDS is required.' 1>&2
  exit 1
fi

if [[ -z "${ADMIN}" ]]; then
  echo '$ADMIN is required.' 1>&2
  exit 1
fi

if [[ -z "${DATASET_LOCATION}" ]]; then
  echo '$DATASET_LOCATION is required.' 1>&2
  exit 1
fi

if [[ -z "${DATASET_NAME}" ]]; then
  echo '$DATASET_NAME is required.' 1>&2
  exit 1
fi

export RUN_MERCHANT_EXCELLENCE="${RUN_MERCHANT_EXCELLENCE:-false}"

# TODO: This could be better if I could just compute the image path with terraform somehow
export BUILD_TAG_BASE="${DATAFLOW_REGION}-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT}/${IMAGES_REPO}"
export STAGING_DIR="${DATAFLOW_STAGING_BUCKET}${DATASET_NAME}"

# TODO: Parameterize secret name
# NOTE: If image is changed, change here, too, and pass to container
gcloud run jobs deploy ads-oneshop-job \
    --project "${GOOGLE_CLOUD_PROJECT}" \
    --region "${DATAFLOW_REGION}" \
    --service-account="${CLOUD_RUN_SERVICE_ACCOUNT}" \
    --image "${BUILD_TAG_BASE}/cloud_run_job:latest" \
    --cpu 4 \
    --memory 16Gi \
    --task-timeout 4h \
    --max-retries 1 \
    --set-env-vars "USE_DATAFLOW_RUNNER=true" \
    --set-env-vars "PROJECT_NAME=${GOOGLE_CLOUD_PROJECT}" \
    --set-env-vars "GOOGLE_ADS_USE_PROTO_PLUS=False" \
    --set-secrets  "GOOGLE_ADS_CLIENT_ID=google_ads_client_id:latest" \
    --set-secrets  "GOOGLE_ADS_CLIENT_SECRET=google_ads_client_secret:latest" \
    --set-secrets  "GOOGLE_ADS_DEVELOPER_TOKEN=google_ads_developer_token:latest" \
    --set-env-vars "DATAFLOW_SERVICE_ACCOUNT=${DATAFLOW_SERVICE_ACCOUNT}" \
    --set-env-vars "DATAFLOW_REGION=${DATAFLOW_REGION}" \
    --set-env-vars "DATAFLOW_TEMP_LOCATION=${DATAFLOW_TEMP_BUCKET}" \
    --set-env-vars "IMAGES_REPO=${IMAGES_REPO}" \
    --set-env-vars "^@^CUSTOMER_IDS=${CUSTOMER_IDS}" \
    --set-env-vars "^@^MERCHANT_IDS=${MERCHANT_IDS}" \
    --set-secrets  "GOOGLE_ADS_REFRESH_TOKEN=google_ads_refresh_token:latest" \
    --set-env-vars "ADMIN=${ADMIN}" \
    --set-env-vars "DATASET_LOCATION=${DATASET_LOCATION}" \
    --set-env-vars "DATASET_NAME=${DATASET_NAME}" \
    --set-env-vars "STAGING_DIR=${STAGING_DIR}" \
    --set-env-vars "RUN_MERCHANT_EXCELLENCE=${RUN_MERCHANT_EXCELLENCE}"
