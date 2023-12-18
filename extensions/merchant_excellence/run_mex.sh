#!/bin/bash

set -e

if [[ -z "$REGION" ]]; then
    echo '$REGION is required.' 1>&2
    exit 1
fi

create_views() {
  BQ_FLAGS_BASE="--location=${DATASET_LOCATION} --nouse_legacy_sql"
  CURRENT_DATE="$(date '+%Y%M%d')"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./offer_list.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./account_list.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./benchmark_scores.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./all_metrics.sql)"
  bq query $BQ_FLAGS_BASE --destination_table ${PROJECT_NAME}:${DATASET_NAME}.MEX_All_Metrics_historical\$${CURRENT_DATE} --replace=true "$(envsubst < ./all_metrics_historical.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./offer_funnel.sql)"
  bq query $BQ_FLAGS_BASE --destination_table ${PROJECT_NAME}:${DATASET_NAME}.MEX_Offer_Funnel_historical\$${CURRENT_DATE} --replace=true "$(envsubst < ./offer_funnel_historical.sql)"
}

create_views
