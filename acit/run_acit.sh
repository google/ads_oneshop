#!/bin/bash

set -e

if [[ -z "$CUSTOMER_ID" ]]; then
    echo '$CUSTOMER_ID is required' 1>&2
    exit 1
fi

if [[ -z "$MERCHANT_ID" ]]; then
    echo '$MERCHANT_ID is required' 1>&2
    exit 1
fi

if [[ -z "$STAGING_DIR" ]]; then
    echo '$STAGING_DIR is required' 1>&2
    exit 1
fi

if [[ -z "$PROJECT_NAME" ]]; then
    echo '$PROJECT_NAME is required' 1>&2
    exit 1
fi

if [[ -z "$DATASET_NAME" ]]; then
    echo '$DATASET_NAME is required' 1>&2
    exit 1
fi

if [[ -z "$DATASET_LOCATION" ]]; then
    echo '$DATASET_LOCATION (e.g. "US") is required' 1>&2
    exit 1
fi



SOURCES_DIR="${STAGING_DIR}/sources"
SINKS_DIR="${STAGING_DIR}/sinks"
BQ_DIR="${STAGING_DIR}/bq"

pull_data() {
  echo "Pulling data"
  rm -rf "${SOURCES_DIR}" && mkdir -p "${SOURCES_DIR}"
  python -m acit.acit \
    --customer_id="$CUSTOMER_ID" \
    --merchant_id="$MERCHANT_ID" \
    --output="${SOURCES_DIR}"
  echo "Data saved to ${SOURCES_DIR}"
}

run_pipeline() {
  echo "Running product pipeline"
  rm -rf "${SINKS_DIR}" && mkdir -p "${SINKS_DIR}"
  python -m acit.create_base_tables \
    --output="${SINKS_DIR}/wide_products_table.jsonlines" \
    --source_dir="${SOURCES_DIR}" \
    -- \
    --runner=direct
  echo "Product data saved to ${SINKS_DIR}"
}

upload_to_bq() {
  rm -rf "${BQ_DIR}" && mkdir -p "${BQ_DIR}"
  cat $(find "${SOURCES_DIR}" -type f | grep accounts) > "${BQ_DIR}/accounts.jsonlines"
  cat $(find "${SOURCES_DIR}" -type f | grep shippingsettings) > "${BQ_DIR}/shippingsettings.jsonlines"
  cat $(find "${SOURCES_DIR}" -type f | grep liasettings) > "${BQ_DIR}/liasettings.jsonlines"
  cat $(find "${SOURCES_DIR}" -type f | grep performance) > "${BQ_DIR}/performance.jsonlines"
  cat $(find "${SOURCES_DIR}" -type f | grep language) > "${BQ_DIR}/language.jsonlines"
  cat $(find "${SINKS_DIR}" -type f | grep wide_products_table ) > "${BQ_DIR}/products.jsonlines"
  BQ_FLAGS_BASE="--location=${DATASET_LOCATION} \
    --replace=true \
    --source_format=NEWLINE_DELIMITED_JSON"

  BQ_FLAGS="--autodetect ${BQ_FLAGS_BASE}"

  bq load $BQ_FLAGS \
    "${PROJECT_NAME}:${DATASET_NAME}.accounts" \
    "${BQ_DIR}/accounts.jsonlines"

  bq load $BQ_FLAGS \
    "${PROJECT_NAME}:${DATASET_NAME}.shippingsettings" \
    "${BQ_DIR}/shippingsettings.jsonlines"

  bq load $BQ_FLAGS \
    "${PROJECT_NAME}:${DATASET_NAME}.liasettings" \
    "${BQ_DIR}/liasettings.jsonlines"

  bq load $BQ_FLAGS \
    "${PROJECT_NAME}:${DATASET_NAME}.performance" \
    "${BQ_DIR}/performance.jsonlines"

  bq load $BQ_FLAGS \
    "${PROJECT_NAME}:${DATASET_NAME}.language" \
    "${BQ_DIR}/language.jsonlines"

  bq load $BQ_FLAGS_BASE \
    "${PROJECT_NAME}:${DATASET_NAME}.products" \
    "${BQ_DIR}/products.jsonlines" \
    acit/schemas/acit/Products.schema
}

create_views() {
  BQ_FLAGS_BASE="--location=${DATASET_LOCATION} --nouse_legacy_sql"
  bq query $BQ_FLAGS_BASE "$(envsubst < acit/views/main_view.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < acit/views/disapprovals_view.sql)"
}

pull_data
run_pipeline
upload_to_bq
create_views
