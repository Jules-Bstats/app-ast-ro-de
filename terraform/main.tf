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
