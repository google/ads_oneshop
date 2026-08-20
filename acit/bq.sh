#!/bin/bash
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

#
# Shell functions for interacting with BigQuery.
#
# Requires the following environment variables:
# - PROJECT_NAME: The Google Cloud project name.
# - DATASET_NAME: The BigQuery dataset name.
# - DATASET_LOCATION: The location of the BigQuery dataset.

set -e

# TTL for temporary tables. Default is 60 days.
TTL="$(( 60 * 60 * 24 * 60 ))"

bq::_refresh_auth() {
  local token
  token=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" -H "Metadata-Flavor: Google" | jq -r '.access_token')
  if [[ -z "${token}" || "${token}" == "null" ]]; then
    token=$(gcloud auth application-default print-access-token)
  fi

  if [[ -z "${token}" || "${token}" == "null" ]]; then
    echo "Error: running a container outside of GCP is not supported without configured application default credentials." >&2
    exit 1
  fi
  echo "${token}"
}

# ---
# Public functions
# ---

# Creates a BigQuery dataset if it doesn't already exist.
#
# Uses the following environment variables:
# - PROJECT_NAME
# - DATASET_NAME
# - DATASET_LOCATION
bq::create_dataset() {
  # TODO(cbartz): Change this to "bq::ensure_dataset".
  #
  # Check for dataset existence before attempting creation.
  # Return failure if creation fails for reasons other than the dataset already existing.
  CLOUDSDK_AUTH_ACCESS_TOKEN=$(bq::_refresh_auth)
  trap 'unset CLOUDSDK_AUTH_ACCESS_TOKEN' RETURN

  curl -s -X POST "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_NAME}/datasets" \
    -H "Authorization: Bearer ${CLOUDSDK_AUTH_ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"datasetReference\": {\"datasetId\": \"${DATASET_NAME}\"}, \"location\": \"${DATASET_LOCATION}\"}" > /dev/null || true
}

# Loads data from a source URI into a BigQuery table.
#
# If a schema is not provided, schema autodetection will be enabled.
#
# Positional parameters:
#   source_uri: The GCS URI of the data to load.
#   table_id: The ID of the destination table.
#   format: The format of the source data (e.g., "NEWLINE_DELIMITED_JSON", "CSV", "AVRO").
#   ttl_seconds: The time-to-live for the table in seconds.
#   schema_path: The path to the schema file for the data.
#
# Uses the following environment variables:
# - PROJECT_NAME
# - DATASET_NAME
# - CLOUDSDK_AUTH_ACCESS_TOKEN
bq::load() {
  local CLOUDSDK_AUTH_ACCESS_TOKEN
  CLOUDSDK_AUTH_ACCESS_TOKEN=$(bq::_refresh_auth)
  trap 'unset CLOUDSDK_AUTH_ACCESS_TOKEN' RETURN

  local source_uri="$1"
  local table_id="$2"
  local format="$3"
  local ttl_seconds="$4"
  local schema_path="$5"

  local job_id
  job_id=$(bq::_load "${source_uri}" "${table_id}" "${format}" "${schema_path}")
  if [[ -z "${job_id}" || "${job_id}" == "null" ]]; then
    echo "Failed to start BigQuery load job." >&2
    return 1
  fi
  echo "BigQuery job started: ${job_id}"

  if ! bq::_poll_job "${job_id}"; then
    echo "BigQuery job failed or timed out: ${job_id}" >&2
    return 1
  fi
  echo "BigQuery job ${job_id} completed successfully."

  if ! bq::_patch_table_expiration "${table_id}" "${ttl_seconds}"; then
     echo "Failed to set table expiration for ${table_id}" >&2
     return 1
  fi
  echo "Successfully set table expiration for ${table_id}."
  return 0
}

# Runs a BigQuery DDL script.
#
# Reads the script from standard input.
#
# Positional parameters:
#   destination_table: Optional. The ID of the destination table for query results.
#
# Uses the following environment variables:
# - PROJECT_NAME
# - DATASET_NAME
# - DATASET_LOCATION
bq::run_ddl() {
  local CLOUDSDK_AUTH_ACCESS_TOKEN
  CLOUDSDK_AUTH_ACCESS_TOKEN=$(bq::_refresh_auth)
  trap 'unset CLOUDSDK_AUTH_ACCESS_TOKEN' RETURN

  local destination_table="$1"
  local query
  query=$(cat -)

  local destination_table_obj="{}"
  local write_disposition_obj="{}"
  if [[ -n "${destination_table}" ]]; then
    destination_table_obj=$(jq -n --arg project_name "${PROJECT_NAME}" --arg dataset_name "${DATASET_NAME}" --arg table_id "${destination_table}" \
    '{
      "destinationTable": {
        "projectId": $project_name,
        "datasetId": $dataset_name,
        "tableId": $table_id
      }
    }')
    write_disposition_obj='{"writeDisposition": "WRITE_TRUNCATE"}'
  fi

  local payload
  payload=$(jq -n \
    --arg query "$query" \
    --arg location "$DATASET_LOCATION" \
    --arg project_name "$PROJECT_NAME" \
    --arg dataset_name "$DATASET_NAME" \
    --argjson destination_table_obj "${destination_table_obj}" \
    --argjson write_disposition_obj "${write_disposition_obj}" \
    '{
      "configuration": {
        "query": ({
          "query": $query,
          "useLegacySql": false,
          "defaultDataset": {
            "datasetId": $dataset_name,
            "projectId": $project_name
          },
          "flattenResults": true,
          "allowLargeResults": true,
          "priority": "BATCH"
        } + $destination_table_obj + $write_disposition_obj)
      }
    }')

  local job_id
  job_id=$(curl -s -X POST "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_NAME}/jobs" \
    -H "Authorization: Bearer ${CLOUDSDK_AUTH_ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${payload}" | jq -r '.jobReference.jobId')

  if [[ -z "${job_id}" || "${job_id}" == "null" ]]; then
    echo "Failed to start BigQuery query job." >&2
    return 1
  fi
  echo "BigQuery job started: ${job_id}"

  if ! bq::_poll_job "${job_id}"; then
    echo "BigQuery job failed or timed out: ${job_id}" >&2
    return 1
  fi
  echo "BigQuery job ${job_id} completed successfully."
}


# ---
# Private functions
# ---

# Starts a BigQuery load job.
#
# This is a private function.
#
# Positional parameters:
#   source_uri: The GCS URI of the data to load.
#   table_id: The ID of the destination table.
#   schema_path: The path to the schema file for the data. If not provided,
#     autodetection will be used.
#
# Echos a bare job_id on success.
#
# Uses the following environment variables:
# - PROJECT_NAME
# - DATASET_NAME
# - CLOUDSDK_AUTH_ACCESS_TOKEN
bq::_load() {
  local source_uri="$1"
  local table_id="$2"
  local format="$3"
  # Positional parameters
  local schema_path="$4"

  local schema_block=""
  local autodetect_flag=""

  if [[ -z "${schema_path}" ]]; then
    autodetect_flag="\"autodetect\": true"
  else
    local schema_fields=$(cat "${schema_path}")
    schema_block="\"schema\": {\"fields\": ${schema_fields}},"
    autodetect_flag="\"autodetect\": false"
  fi

  local skip_rows_block=""
  if [[ "${format}" == "CSV" ]]; then
    skip_rows_block='"skipLeadingRows": 1,'
  fi

  local job_response
  if [[ -f "${source_uri}" ]]; then
    local job_config
    job_config=$(cat <<EOF
{
  "configuration": {
    "load": {
      ${schema_block}
      "destinationTable": {
        "projectId": "${PROJECT_NAME}",
        "datasetId": "${DATASET_NAME}",
        "tableId": "${table_id}"
      },
      "sourceFormat": "${format}",
      "ignoreUnknownValues": true,
      ${skip_rows_block}
      "writeDisposition": "WRITE_TRUNCATE",
      ${autodetect_flag}
    }
  }
}
EOF
)
    local upload_url
    upload_url=$(curl -si -X POST "https://bigquery.googleapis.com/upload/bigquery/v2/projects/${PROJECT_NAME}/jobs?uploadType=resumable" \
      -H "Authorization: Bearer ${CLOUDSDK_AUTH_ACCESS_TOKEN}" \
      -H "Content-Type: application/json; charset=UTF-8" \
      -d "${job_config}" | grep -i "location:" | awk '{print $2}' | tr -d '\r')

    if [[ -z "${upload_url}" ]]; then
      echo "Failed to get upload URL for BigQuery job." >&2
      return 1
    fi

    job_response=$(curl -s -X PUT "${upload_url}" \
      -H "Content-Type: application/octet-stream" \
      --data-binary "@${source_uri}")
  else
    job_response=$(curl -s -X POST "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_NAME}/jobs" \
      -H "Authorization: Bearer ${CLOUDSDK_AUTH_ACCESS_TOKEN}" \
      -H "Content-Type: application/json" \
      -d @- << EOF
{
  "configuration": {
    "load": {
      "sourceUris": ["${source_uri}"],
      ${schema_block}
      "destinationTable": {
        "projectId": "${PROJECT_NAME}",
        "datasetId": "${DATASET_NAME}",
        "tableId": "${table_id}"
      },
      "sourceFormat": "${format}",
      "ignoreUnknownValues": true,
      ${skip_rows_block}
      "writeDisposition": "WRITE_TRUNCATE",
      ${autodetect_flag}
    }
  }
}
EOF
)
  fi
  local res_job_id
  res_job_id=$(echo "${job_response}" | jq -r '.jobReference.jobId')
  if [[ -z "${res_job_id}" || "${res_job_id}" == "null" ]]; then
    echo "BigQuery API Error Response: ${job_response}" >&2
    return 1
  fi
  echo "${res_job_id}"
}

# Waits for a BigQuery job to complete.
#
# This is a private function.
#
# Positional parameters:
#  - job_id: The ID of the job to poll.
#
# Returns 0 on success, 1 on failure or timeout.
#
# Uses the following environment variables:
# - PROJECT_NAME
# - CLOUDSDK_AUTH_ACCESS_TOKEN
bq::_poll_job() {
  local job_id="$1"
  local seconds=0
  while true; do
    local job_status
    job_status=$(curl -s -X GET "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_NAME}/jobs/${job_id}" \
      -H "Authorization: Bearer ${CLOUDSDK_AUTH_ACCESS_TOKEN}" \
      -H "Content-Type: application/json")
    local status
    status=$(echo "${job_status}" | jq -r '.status.state')
    echo "Job status: ${status}"
    if [[ "${status}" == "DONE" ]]; then
      if [[ $(echo "${job_status}" | jq -r '.status.errorResult') != "null" ]]; then
        echo "Job failed:" >&2
        echo "${job_status}" | jq -r '.status.errorResult.message' >&2
        echo "Detailed errors:" >&2
        echo "${job_status}" | jq -r '.status.errors[]?.message' >&2
        return 1
      fi
      return 0
    fi
    if (( seconds > 60 )); then
      echo "Job timed out." >&2
      return 1
    fi
    sleep 5
    seconds=$((seconds + 5))
  done
}

# Updates the expiration time of a BigQuery table.
#
# This is a private function.
#
# Positional parameters:
#  - table_id: The ID of the table to update.
#  - ttl_seconds: The time-to-live in seconds.
#
# Returns the exit code of the curl command.
#
# Uses the following environment variables:
# - PROJECT_NAME
# - DATASET_NAME
# - CLOUDSDK_AUTH_ACCESS_TOKEN
bq::_patch_table_expiration() {
    local table_id="$1"
    local ttl_seconds="$2"
    local expiration_time
    expiration_time=$(( ($(date +%s) + ttl_seconds) * 1000 ))

    curl -s -X PATCH "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_NAME}/datasets/${DATASET_NAME}/tables/${table_id}" \
      -H "Authorization: Bearer ${CLOUDSDK_AUTH_ACCESS_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"expirationTime\": \"${expiration_time}\"}" > /dev/null
}

main() {
  if [[ "$#" -ne 3 ]]; then
    echo "Usage: $(basename "$0") <source_uri> <table_id> <schema_path>"
    echo ""
    echo "Loads data from a source URI into a BigQuery table."
    echo ""
    echo "Requires the following environment variables:"
    echo "  - PROJECT_NAME: The Google Cloud project name."
    echo "  - DATASET_NAME: The BigQuery dataset name."
    echo "  - DATASET_LOCATION: The location of the BigQuery dataset."
    exit 1
  fi

  local source_uri="$1"
  local table_id="$2"
  local schema_path="$3"

  bq::create_dataset
  bq::load "${source_uri}" "${table_id}" "NEWLINE_DELIMITED_JSON" "${TTL}" "${schema_path}"
}

# Only run main if not being sourced.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
