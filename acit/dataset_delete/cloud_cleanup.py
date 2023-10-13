import google.cloud.bigquery as bigquery
from google.cloud.exceptions import NotFound
import google.cloud.storage as storage
import acit.config.constants as constants


def bq_delete_data_blobs(bucket_name, data_blob):
  try:
    storage_client = storage.Client()
    blob = storage_client.get_bucket(bucket_name).blob(data_blob)

    generation_match_precondition = None

    blob.reload()
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    print(f"Data Storage File:{data_blob} deleted.")
  except NotFound:
    # Nothing to delete
    pass


def bq_delete_bucket(bucket_name):
  try:
    storage_client = storage.Client()
    storage_client.get_bucket(bucket_name).delete()

    print(f"Bucket:{bucket_name} deleted")
  except NotFound:
    # Nothing to delete
    pass


def bq_delete_table(client, project_name, dataset, table):
  try:
    table_id = "{}.{}.{}".format(project_name, dataset, table)

    client.delete_table(table_id)
    print(f"Deleted Table '{table_id}'")
  except NotFound:
    # Nothing to delete
    pass


if __name__ == "__main__":
  client = bigquery.Client()

  bq_delete_data_blobs(constants.STORAGE_BUCKET, constants.ADS_STORAGE_BLOB)
  bq_delete_data_blobs(
      constants.STORAGE_BUCKET, constants.PRODUCTS_STORAGE_BLOB
  )
  bq_delete_data_blobs(constants.STORAGE_BUCKET, constants.GEO_STORAGE_BLOB)
  bq_delete_data_blobs(
      constants.STORAGE_BUCKET, constants.LANGUAGES_STORAGE_BLOB
  )
  bq_delete_data_blobs(
      constants.STORAGE_BUCKET, constants.PRODUCTSTATUS_STORAGE_BLOB
  )
  bq_delete_bucket(constants.STORAGE_BUCKET)

  # Delete the non-final tables
  bq_delete_table(
      client,
      constants.PROJECT_NAME,
      constants.DATASET,
      constants.TABLE_NAME_LANGUAGES,
  )
  bq_delete_table(
      client,
      constants.PROJECT_NAME,
      constants.DATASET,
      constants.TABLE_NAME_GEO,
  )
  bq_delete_table(
      client,
      constants.PROJECT_NAME,
      constants.DATASET,
      constants.TABLE_NAME_PRODUCTS,
  )
  bq_delete_table(
      client,
      constants.PROJECT_NAME,
      constants.DATASET,
      constants.TABLE_NAME_PRODUCTSTATUS,
  )
  bq_delete_table(
      client,
      constants.PROJECT_NAME,
      constants.DATASET,
      constants.TABLE_NAME_ADS,
  )
