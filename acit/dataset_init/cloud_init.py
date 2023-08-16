"""Load data into BQ."""

import google.cloud.bigquery as bigquery
from google.cloud.exceptions import NotFound
import google.cloud.storage as storage
import acit.config.constants as constants


def bq_create_dataset(client: bigquery.Client, project_name: str,
                      dataset_ref: str) -> None:
  dataset_id = f"{project_name}.{dataset_ref}"
  try:
    dataset = client.get_dataset(dataset_id)
    print(f'Dataset {dataset} already exists.')
  except NotFound:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'US'
    dataset = client.create_dataset(dataset)
    print(f'Dataset {dataset.dataset_id} created.')


def bq_create_storage_bucket(bucket_name: str) -> None:
  storage_client = storage.Client()

  try:
    bucket = storage_client.get_bucket(bucket_name)
    print(f'Storage Bucket {bucket} already exists.')
  except NotFound:
    bucket = storage_client.create_bucket(bucket_name, location="us")
    bucket.storage_class = "STANDARD"
    print(
        f"Created bucket {bucket.name} in {bucket.location} with storage class {bucket.storage_class}"
    )


def bq_upload_blob(bucket_name: str, source_file_name: str,
                   destination_blob_name: str) -> None:
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)
  print(f'Data Blob {destination_blob_name} Created!')


if __name__ == "__main__":
  bigquery_client = bigquery.Client()

  bq_create_dataset(bigquery_client, constants.PROJECT_NAME, constants.DATASET)
  bq_create_storage_bucket(constants.STORAGE_BUCKET)

  bq_upload_blob(constants.STORAGE_BUCKET,
                 "./acit/api_datasets/data/products_data/products.json",
                 constants.PRODUCTS_STORAGE_FILE)
  bq_upload_blob(constants.STORAGE_BUCKET,
                 "./acit/api_datasets/data/products_data/productstatus.json",
                 constants.PRODUCTSTATUS_STORAGE_FILE)
  bq_upload_blob(constants.STORAGE_BUCKET,
                 "./acit/api_datasets/data/geo_data/geo_targets.csv",
                 constants.GEO_STORAGE_FILE)
  bq_upload_blob(constants.STORAGE_BUCKET,
                 "./acit/api_datasets/data/lang_data/language_codes.csv",
                 constants.LANGUAGES_STORAGE_FILE)
  bq_upload_blob(constants.STORAGE_BUCKET,
                 "./acit/api_datasets/data/ads_data/impressions.csv",
                 constants.ADS_STORAGE_FILE)
