resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-raw"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
}

locals {
  bq_datasets = {
    asteroides_raw     = "Données brutes issues de l'API NASA NeoWs"
    asteroides_staging = "Données nettoyées et typées (modèles dbt staging)"
    asteroides_marts   = "Tables finales prêtes pour l'analyse (modèles dbt marts)"
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.bq_datasets

  dataset_id                 = each.key
  description                = each.value
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_secret_manager_secret" "nasa_api_key" {
  secret_id = "nasa-api-key"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}
resource "google_service_account" "ingestion" {
  account_id   = "sa-ingestion"
  display_name = "Compte de service d'ingestion NeoWs"
}

resource "google_storage_bucket_iam_member" "ingestion_bucket" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.ingestion.email}"
}

resource "google_secret_manager_secret_iam_member" "ingestion_secret" {
  secret_id = google_secret_manager_secret.nasa_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ingestion.email}"
}
