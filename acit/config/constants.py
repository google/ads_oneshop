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

import os

PROJECT_NAME = os.environ["GCP_PROJECT_ID"]

DATASET = "ACIT_Dataset_Repository"

ADS_STORAGE_FILE = "./acit/api_datasets/data/ads_data/impressions.csv"
PRODUCTS_STORAGE_FILE = "./acit/api_datasets/data/products_data/products.json"
PRODUCTSTATUS_STORAGE_FILE = (
    "./acit/api_datasets/data/products_data/productstatus.json"
)
GEO_STORAGE_FILE = "./acit/api_datasets/data/geo_data/geo_targets.csv"
LANGUAGES_STORAGE_FILE = "./acit/api_datasets/data/lang_data/language_codes.csv"

ADS_STORAGE_BLOB = "Ads_Storage_Data"
PRODUCTS_STORAGE_BLOB = "Products_Storage_Data"
PRODUCTSTATUS_STORAGE_BLOB = "Product_Status_Storage_Data"
GEO_STORAGE_BLOB = "Geo_Storage_Data"
LANGUAGES_STORAGE_BLOB = "Languages_Storage_Data"

TABLE_NAME_ADS = "ACIT_Ads_Data"
TABLE_NAME_PRODUCTS = "ACIT_Products_Data"
TABLE_NAME_PRODUCTSTATUS = "ACIT_ProductStatus_Data"
TABLE_NAME_BENCHMARKS = "ACIT_Benchmarks_Data"
TABLE_NAME_GEO = "Geo_Data"
TABLE_NAME_LANGUAGES = "Languages_Data"

STORAGE_BUCKET = os.environ["GCS_STORAGE_BUCKET"]
