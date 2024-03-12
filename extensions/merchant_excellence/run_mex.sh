#!/bin/bash

set -e

if [[ -z "$REGION" ]]; then
    echo '$REGION is required.' 1>&2
    exit 1
fi

create_views() {
  BQ_FLAGS_BASE="--location=${DATASET_LOCATION} --nouse_legacy_sql"
  CURRENT_DATE="$(date '+%Y%m%d')"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./extensions/merchant_excellence/offer_list.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./extensions/merchant_excellence/account_list.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./extensions/merchant_excellence/benchmark_scores.sql)"
  bq query $BQ_FLAGS_BASE "$(envsubst < ./extensions/merchant_excellence/all_metrics.sql)"
  bq query $BQ_FLAGS_BASE --destination_table ${PROJECT_NAME}:${DATASET_NAME}.MEX_All_Metrics_historical\$${CURRENT_DATE} --replace=true "$(envsubst < ./extensions/merchant_excellence/all_metrics_historical.sql)"

  bq query $BQ_FLAGS_BASE "$(envsubst < ./extensions/merchant_excellence/offer_funnel.sql)"
  bq query $BQ_FLAGS_BASE --destination_table ${PROJECT_NAME}:${DATASET_NAME}.MEX_Offer_Funnel_historical\$${CURRENT_DATE} --replace=true "$(envsubst < ./extensions/merchant_excellence/offer_funnel_historical.sql)"
}

create_views
