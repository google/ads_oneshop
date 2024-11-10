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

output "cloud_build_sa" {
  value = google_service_account.cloud_build.id
}

output "cloud_run_sa" {
  value = google_service_account.cloud_run.id
}

output "dataflow_sa" {
  value = google_service_account.dataflow.id
}

output "region" {
  value = var.region
}

output "cloud_build_logs_url" {
  value = google_storage_bucket.cloud_build_logs.url
}

output "images_repo" {
  value = google_artifact_registry_repository.container_repo.name
}

# TODO: Add output for deployments JSON
