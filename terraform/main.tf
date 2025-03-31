terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.50.0" # Use a recent version
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  # Removed impersonate_service_account here.
  # Terraform will use the credentials of the user/SA running it.
}

# --- Variables ---

variable "project_id" {
  description = "The GCP project ID."
  type        = string
  default     = "original-glider-455309-s7"
}

variable "region" {
  description = "The GCP region for resources like Dataproc and potentially Composer."
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "The location for BigQuery datasets (e.g., US, EU, region)."
  type        = string
  default     = "US" # Matching the original GCS bucket location
}

variable "gcs_location" {
  description = "The location for GCS buckets (e.g., US, EU, region)."
  type        = string
  default     = "US" # Multi-region often suitable for Data Lakes
}

variable "dataproc_sa_email" {
  description = "Email of the pre-existing Dataproc service account."
  type        = string
  default     = "dataproc-sa@original-glider-455309-s7.iam.gserviceaccount.com"
}

variable "de_project_sa_email" {
  description = "Email of the pre-existing general Data Engineering service account (e.g., for Airflow)."
  type        = string
  default     = "de-project-service-account@original-glider-455309-s7.iam.gserviceaccount.com"
}


# --- Cloud Storage (Data Lake & Support Buckets) ---

resource "google_storage_bucket" "data_lake_raw" {
  name          = "${var.project_id}-datalake-raw" # Project ID helps ensure uniqueness
  location      = var.gcs_location
  force_destroy = true # Use with caution in production

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # Example: Delete raw files older than 30 days
      # with_state = "ANY" # Applies to live and archived objects
    }
  }

  # Optional: Versioning
  # versioning {
  #   enabled = true
  # }
}

resource "google_storage_bucket" "data_lake_processed" {
  name          = "${var.project_id}-datalake-processed"
  location      = var.gcs_location
  force_destroy = true # Use with caution in production

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "airflow_dags" {
  name          = "${var.project_id}-airflow-dags"
  location      = var.region # Often best to co-locate with Composer/Airflow workers
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "spark_scripts" {
  name          = "${var.project_id}-spark-scripts"
  location      = var.gcs_location # Or region, depending on access patterns
  force_destroy = true

  uniform_bucket_level_access = true
}

# --- BigQuery ---

resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id                  = "open_payments_staging" # More descriptive name
  project                     = var.project_id
  location                    = var.bq_location
  delete_contents_on_destroy = true # For development convenience
}

resource "google_bigquery_dataset" "analytics_dataset" {
  dataset_id                  = "open_payments_analytics" # More descriptive name
  project                     = var.project_id
  location                    = var.bq_location
  delete_contents_on_destroy = true # For development convenience
}

# --- Dataproc ---

resource "google_dataproc_cluster" "etl_cluster" {
  name    = "etl-cluster-${lower(var.region)}" # Include region for potential multi-region setup clarity
  region  = var.region
  project = var.project_id

  cluster_config {
    # Specify the Service Account for the cluster VMs
    gce_cluster_config {
      service_account = var.dataproc_sa_email
      # Define API access scopes for the VM instances.
      # 'cloud-platform' provides broad access, controlled by IAM roles.
      # Alternatively, use more specific scopes like:
      # ["https://www.googleapis.com/auth/cloud.platform", "https://www.googleapis.com/auth/devstorage.read_write", "https://www.googleapis.com/auth/bigquery"]
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
      tags = ["dataproc", "etl-cluster"] # Optional tags for firewall rules etc.
    }

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2" # Consider n2d or e2 for better cost/performance
      # disk_config {
      #   boot_disk_type    = "pd-standard"
      #   boot_disk_size_gb = 100
      # }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2" # Consider n2d or e2
      # disk_config {
      #   boot_disk_type    = "pd-standard"
      #   boot_disk_size_gb = 100
      # }
    }

    # Optional: Software configuration (e.g., enable components)
    # software_config {
    #   image_version = "2.1-debian11" # Specify image version
    #   optional_components = ["JUPYTER"]
    # }

    # Optional: Initialization actions
    # initialization_action {
    #   executable_file = "gs://path/to/your/init-script.sh"
    #   execution_timeout = "600s"
    # }
  }

  # Prevent accidental deletion if cluster is in use
  # lifecycle {
  #   prevent_destroy = true
  # }

  # Define dependency: IAM bindings must be applied before cluster creation might need them
  depends_on = [
    # Correctly reference the BUCKET and DATASET IAM members by their actual resource type
    google_storage_bucket_iam_member.dataproc_sa_gcs_raw_reader,
    google_storage_bucket_iam_member.dataproc_sa_gcs_processed_writer,
    google_bigquery_dataset_iam_member.dataproc_sa_bq_staging_writer,
    # This one was already correct as it's a project-level role
    google_project_iam_member.dataproc_sa_dataproc_worker
    # Add others if needed, using their correct resource type (google_storage_bucket_iam_member, google_bigquery_dataset_iam_member, etc.)
  ]

}


# --- IAM Permissions ---
# Grant roles to the Dataproc Service Account (dataproc-sa)

# Permissions needed by Dataproc cluster VMs themselves (if not covered by broader project roles)
# Role needed to function as a Dataproc worker/master node VM
resource "google_project_iam_member" "dataproc_sa_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${var.dataproc_sa_email}"
}

# GCS: Read from Raw Bucket
resource "google_storage_bucket_iam_member" "dataproc_sa_gcs_raw_reader" {
  bucket = google_storage_bucket.data_lake_raw.name
  role   = "roles/storage.objectViewer" # Read-only access
  member = "serviceAccount:${var.dataproc_sa_email}"
}

# GCS: Write to Processed Bucket
resource "google_storage_bucket_iam_member" "dataproc_sa_gcs_processed_writer" {
  bucket = google_storage_bucket.data_lake_processed.name
  role   = "roles/storage.objectCreator" # Write access
  member = "serviceAccount:${var.dataproc_sa_email}"
  # Add roles/storage.legacyBucketWriter if needed by specific Hadoop connectors
}

# BigQuery: Write to Staging Dataset (e.g., load data, create/overwrite tables)
resource "google_bigquery_dataset_iam_member" "dataproc_sa_bq_staging_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging_dataset.dataset_id
  role       = "roles/bigquery.dataEditor" # Allows read/write data & tables
  member     = "serviceAccount:${var.dataproc_sa_email}"
}

# BigQuery: Read Staging Data (potentially needed by Spark BQ connector if reading back)
resource "google_bigquery_dataset_iam_member" "dataproc_sa_bq_staging_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${var.dataproc_sa_email}"
}

# Grant roles to the Data Engineering Project Service Account (DE-project-service-account)
# Assuming this SA will be used by Airflow/Composer

# GCS: Access DAGs bucket
resource "google_storage_bucket_iam_member" "de_sa_gcs_dags_access" {
  bucket = google_storage_bucket.airflow_dags.name
  role   = "roles/storage.objectAdmin" # Needs to read/write/delete DAGs
  member = "serviceAccount:${var.de_project_sa_email}"
}

# GCS: Access Spark Scripts bucket (Read needed by Airflow to submit jobs)
resource "google_storage_bucket_iam_member" "de_sa_gcs_scripts_reader" {
  bucket = google_storage_bucket.spark_scripts.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.de_project_sa_email}"
}

# Dataproc: Submit jobs to the cluster
resource "google_project_iam_member" "de_sa_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor" # Allows submitting jobs, managing clusters (adjust if too broad)
  # Alternatives: roles/dataproc.jobSubmitter if only job submission is needed
  member  = "serviceAccount:${var.de_project_sa_email}"
}

# Service Account User: Allow DE SA to act as/impersonate Dataproc SA *if needed* by Airflow operators
# Often needed if the Airflow operator itself doesn't handle SA switching well,
# or if you want the Dataproc job submission itself to use the Dataproc SA's identity directly.
# The DataprocSubmitPySparkJobOperator usually specifies the job's SA, so this might not be strictly needed
# unless Composer's agent needs it.
# resource "google_service_account_iam_member" "de_sa_can_impersonate_dataproc_sa" {
#   service_account_id = "projects/${var.project_id}/serviceAccounts/${var.dataproc_sa_email}"
#   role               = "roles/iam.serviceAccountUser"
#   member             = "serviceAccount:${var.de_project_sa_email}"
# }

# BigQuery: Load jobs (GCS->BQ), Query jobs (dbt runs), manage tables in Staging/Analytics
resource "google_bigquery_dataset_iam_member" "de_sa_bq_staging_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging_dataset.dataset_id
  role       = "roles/bigquery.dataEditor" # Needed for GCSToBigQueryOperator
  member     = "serviceAccount:${var.de_project_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "de_sa_bq_staging_metadata" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging_dataset.dataset_id
  role       = "roles/bigquery.metadataViewer" # To view table metadata
  member     = "serviceAccount:${var.de_project_sa_email}"
}


resource "google_bigquery_dataset_iam_member" "de_sa_bq_analytics_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  role       = "roles/bigquery.dataEditor" # Needed for dbt to create/update tables
  member     = "serviceAccount:${var.de_project_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "de_sa_bq_analytics_metadata" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  role       = "roles/bigquery.metadataViewer" # To view table metadata
  member     = "serviceAccount:${var.de_project_sa_email}"
}


# BigQuery Job User: Allows running jobs (queries, loads) in the project
resource "google_project_iam_member" "de_sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.de_project_sa_email}"
}

# Grant Composer Worker role to the DE Project SA (needed for Composer environment operation)
resource "google_project_iam_member" "de_sa_composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${var.de_project_sa_email}"
}
