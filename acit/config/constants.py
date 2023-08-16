import os

PROJECT_NAME = os.environ['GCP_PROJECT_ID']

DATASET = "ACIT_Dataset_Repository"

ADS_STORAGE_FILE = "Ads_Storage_Data"
PRODUCTS_STORAGE_FILE = "Products_Storage_Data"
PRODUCTSTATUS_STORAGE_FILE = "Product_Status_Storage_Data"
BENCHMARKS_STORAGE_FILE = "Benchmarks_Storage_Data"
GEO_STORAGE_FILE = "Geo_Storage_Data"
LANGUAGES_STORAGE_FILE = "Languages_Storage_Data"

TABLE_NAME_ADS = "ACIT_Ads_Data"
TABLE_NAME_PRODUCTS = "ACIT_Products_Data"
TABLE_NAME_PRODUCTSTATUS = "ACIT_ProductStatus_Data"
TABLE_NAME_BENCHMARKS = "ACIT_Benchmarks_Data"
TABLE_NAME_GEO = "Geo_Data"
TABLE_NAME_LANGUAGES = "Languages_Data"

STORAGE_BUCKET = os.environ['GCS_STORAGE_BUCKET']
