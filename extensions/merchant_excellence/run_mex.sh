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

create_views() {
  # Define parameters for running `bq` commands
  BQ_FLAGS_BASE="--location=${DATASET_LOCATION} \
  --nouse_legacy_sql"
  BQ_FLAGS_CSV="--location=${DATASET_LOCATION} \
  --replace=true \
  --source_format=CSV \
  --autodetect"
  CURRENT_DATE="$(date '+%Y%m%d')"

  # Loading the Benchmark files into BQ Tables
  bq load $BQ_FLAGS_CSV ${PROJECT_NAME}:${DATASET_NAME}.MEX_benchmark_values benchmark/benchmark_values.csv
  bq load $BQ_FLAGS_CSV ${PROJECT_NAME}:${DATASET_NAME}.MEX_benchmark_details benchmark/benchmark_details.csv

  # Running Merchant Excellence queries
  bq query $BQ_FLAGS_BASE < <(envsubst < ./extensions/merchant_excellence/offer_list.sql)
  bq query $BQ_FLAGS_BASE < <(envsubst < ./extensions/merchant_excellence/account_list.sql)
  bq query $BQ_FLAGS_BASE < <(envsubst < ./extensions/merchant_excellence/benchmark_scores.sql)
  bq query $BQ_FLAGS_BASE < <(envsubst < ./extensions/merchant_excellence/all_metrics.sql)
  bq query $BQ_FLAGS_BASE --destination_table ${PROJECT_NAME}:${DATASET_NAME}.MEX_All_Metrics_historical\$${CURRENT_DATE} --replace=true < <(envsubst < ./extensions/merchant_excellence/all_metrics_historical.sql)
  bq query $BQ_FLAGS_BASE < <(envsubst < ./extensions/merchant_excellence/offer_funnel.sql)
  bq query $BQ_FLAGS_BASE --destination_table ${PROJECT_NAME}:${DATASET_NAME}.MEX_Offer_Funnel_historical\$${CURRENT_DATE} --replace=true < <(envsubst < ./extensions/merchant_excellence/offer_funnel_historical.sql)
  bq query $BQ_FLAGS_BASE < <(envsubst < ./extensions/merchant_excellence/ml_data.sql)
}

create_views
