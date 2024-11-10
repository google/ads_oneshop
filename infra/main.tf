# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# APIs

resource "google_project_service" "artifactregistry" {
  project                    = var.project_id
  service                    = "artifactregistry.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "bigquery" {
  project                    = var.project_id
  service                    = "bigquery.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "cloudbuild" {
  project                    = var.project_id
  service                    = "cloudbuild.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "cloudscheduler" {
  project                    = var.project_id
  service                    = "cloudscheduler.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "dataflow" {
  project                    = var.project_id
  service                    = "dataflow.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "iam" {
  project                    = var.project_id
  service                    = "iam.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "run" {
  project                    = var.project_id
  service                    = "run.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "secretmanager" {
  project                    = var.project_id
  service                    = "secretmanager.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


resource "google_project_service" "storage" {
  project                    = var.project_id
  service                    = "storage.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy         = false
}


# Service Accounts

resource "google_service_account" "cloud_build" {
  project      = var.project_id
  account_id   = "oneshop-cloudbuild-sa"
  display_name = "Cloud Build Service Account"
}

resource "google_service_account" "cloud_run" {
  project      = var.project_id
  account_id   = "oneshop-cloudrun-sa"
  display_name = "Cloud Run Service Account"
}

resource "google_service_account" "cloud_scheduler" {
  project      = var.project_id
  account_id   = "oneshop-cloudscheduler-sa"
  display_name = "Cloud Scheduler Service Account"
}

resource "google_service_account" "dataflow" {
  project      = var.project_id
  account_id   = "oneshop-dataflow-sa"
  display_name = "Dataflow Service Account"
}

# TODO: Must specify a bucket for cloud build, control soft delete policy

# Persistence
# TODO: Specify retention policy for all of these

resource "google_artifact_registry_repository" "container_repo" {
  location               = var.region
  repository_id          = "oneshop-images"
  format                 = "docker"
  cleanup_policy_dry_run = true
}

# TODO: validate lenght of bucket names

resource "google_storage_bucket" "dataflow_staging" {
  name                        = "${var.project_id}_oneshop_dataflow_staging"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket" "dataflow_temp" {
  name                        = "${var.project_id}_oneshop_dataflow_temp"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket" "cloud_build_logs" {
  name                        = "${var.project_id}_cloud_build_logs"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

# IAM

resource "google_project_iam_member" "cloud_build_sa_artifact_registry_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.cloud_build.email}"
}

resource "google_project_iam_member" "cloud_build_sa_logging_logs_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloud_build.email}"
}

resource "google_project_iam_member" "cloud_build_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.cloud_build.email}"
}

resource "google_project_iam_member" "cloud_run_sa_bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_sa_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_sa_secret_manager_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_sa_dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_scheduler_sa_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_scheduler.email}"
}

resource "google_project_iam_member" "dataflow_sa_artifact_registry_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "dataflow_sa_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "dataflow_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_service_account_iam_binding" "cloud_run_sa_dataflow_sa_user_binding" {
  service_account_id = google_service_account.dataflow.name
  role               = "roles/iam.serviceAccountUser"

  members = [
    "serviceAccount:${google_service_account.cloud_run.email}"
  ]
}

# TODO: bigquery dataset (create if not exists within image)

# TODO: Deployment config and module
