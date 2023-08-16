#!/bin/bash

set -e

#0) Introduction and User Input Time
echo "Welcome to the ACIT reporting tool. Glad to have you onboard."
echo "Using GCP project $GCP_PROJECTID"
echo "Using GCS storage bucket $GCS_STORAGE_BUCKET"
echo "Using MCC $MCC"

#1) Configure the project
gcloud config set project $GCP_PROJECT_ID

#2) Execute the code that will get us all of the product and ads data
#   But first, delete the impressions file if it exists.
rm -rf ./acit/api_datasets/data/
mkdir -p ./acit/api_datasets/data/ads_data
mkdir -p ./acit/api_datasets/data/geo_data
mkdir -p ./acit/api_datasets/data/lang_data
touch ./acit/api_datasets/data/ads_data/impressions.csv

#2.1) Ads Code:
python -m acit.api_datasets.google_ads.get_performance_data -l $MCC
python -m acit.api_datasets.google_ads.get_geo_targets
python -m acit.api_datasets.google_ads.get_languages

#2.2) Products Code:
mkdir -p ./acit/api_datasets/data/products_data
python -m acit.api_datasets.merchant_center.shopping.content.products.gcs_product_list
python -m acit.api_datasets.merchant_center.shopping.content.products.gcs_product_status_list

pushd ./acit/api_datasets/data/products_data
#2.3) Flatten products JSON into newline-delimited JSON (required for BigQuery import)
cat products.json | jq -c '.[]' > products_flat.json
mv products_flat.json products.json
#2.4) Flatten product status JSON into newline-delimited JSON (required for BigQuery import)
cat productstatus.json | jq -c '.[]' > productstatus_flat.json
mv productstatus_flat.json productstatus.json
popd

#3) Clean up previous files, if they exist
echo "Cleaning up previous tables..."
python -m acit.dataset_delete.cloud_cleanup

#4) Create the initial elements
python -m acit.dataset_init.cloud_init

#5) Execute the code that will feed in all the information to our Cloud Env
python -m acit.dataset_create.data_set_creation

echo "ACIT data pull has successfully completed!"

