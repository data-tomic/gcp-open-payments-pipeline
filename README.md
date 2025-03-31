# GCP End-to-End Open Payments Data Pipeline & Dashboard

## Objective
This project implements an end-to-end data pipeline on Google Cloud Platform (GCP) to process the CMS Open Payments dataset and visualize key insights on a dashboard. It demonstrates proficiency in various data engineering tools and concepts learned during the Data Engineering Zoomcamp.

## Problem Statement
The goal is to create a data pipeline that ingests, processes, and warehouses the CMS Open Payments data for Program Year 2023. Subsequently, a dashboard with at least two visualizations should be developed:
1.  A graph showing the distribution of a categorical variable (e.g., payments by nature or state).
2.  A graph showing the distribution of data over time (e.g., total payment amount per month).

### Dataset
The dataset used is the **CMS Open Payments General Payments Data for Program Year 2023**. This dataset contains detailed information about payments made by applicable manufacturers and group purchasing organizations (GPOs) to physicians and teaching hospitals.
*   **Source:** Centers for Medicare & Medicaid Services (CMS) Open Payments Data
*   **Specific File Used:** `OP_DTL_GNRL_PGYR2023_P01302025.csv` (extracted from the official zip file)
*   **Download Link (Zip):** [https://download.cms.gov/openpayments/PGYR2023_P01302025_01212025.zip](https://download.cms.gov/openpayments/PGYR2023_P01302025_01212025.zip)

## Architecture

This project implements a **batch processing** pipeline. This approach was chosen because the source dataset (Open Payments data) is published periodically (annually) and does not require real-time streaming.

The pipeline follows these steps:

1.  **Infrastructure Provisioning:** Terraform is used to define and create the necessary GCP resources (GCS buckets, BigQuery datasets, Dataproc cluster, Service Accounts, IAM permissions).
2.  **Data Ingestion (Manual Upload):** The raw CSV data (unzipped) is manually uploaded to a designated GCS "raw" bucket (Data Lake - Bronze Layer).
3.  **Orchestration (Airflow):** A Cloud Composer (managed Airflow) environment orchestrates the pipeline defined in a DAG.
4.  **Data Processing (Spark):** The Airflow DAG triggers a PySpark job on a Dataproc cluster. This job reads the raw CSV from GCS, performs basic cleaning (schema enforcement, column selection), and writes the processed data in Parquet format to a "processed" GCS bucket (Data Lake - Silver Layer).
5.  **Data Loading (GCS to BQ):** The DAG uses a `GCSToBigQueryOperator` to load the processed Parquet data from GCS into a BigQuery staging table.
6.  **Data Transformation (BigQuery SQL):** The DAG executes a SQL query within BigQuery to transform the staging data (casting types, handling nulls, selecting final columns) and loads it into a final analytics table, partitioned and clustered for performance. (Data Warehouse - Gold Layer).
7.  **Visualization (Looker Studio):** A Looker Studio dashboard connects to the final BigQuery analytics table to visualize the required insights.


```mermaid
graph LR
    A[Manual Upload: Raw CSV] --> B(GCS Raw Bucket);
    C[Cloud Composer / Airflow DAG] --> D{Dataproc Cluster};
    B -- Read CSV --> D;
    C -- Submit PySpark Job --> D;
    D -- Write Parquet --> E(GCS Processed Bucket);
    C -- Trigger Load --> F(BigQuery Staging Table);
    E -- Load Parquet --> F;
    C -- Trigger Transform --> G(BigQuery Analytics Table);
    F -- Transform SQL --> G;
    G -- Query Data --> H(Looker Studio Dashboard);

    I[Terraform] --> B;
    I --> E;
    I --> D;
    I --> F;
    I --> G;
    I --> J(Service Accounts & IAM);
    I --> C;

## Technologies Used
* **Cloud Provider:** Google Cloud Platform (GCP)

* **Infrastructure as Code (IaC):** Terraform

* **Workflow Orchestration:** Apache Airflow (via Cloud Composer 2)

* **Data Lake:** Google Cloud Storage (GCS)

* **Batch Processing:** Apache Spark (via Dataproc)

* **Data Warehouse:** Google BigQuery

* **Business Intelligence / Visualization:** Looker Studio (formerly Google Data Studio)

* **Language:** Python (for PySpark and Airflow DAG)

* **Data Format:** CSV (raw), Parquet (processed)

## Pipeline Details & Code
### 1. Infrastructure (Terraform)
The infrastructure is defined in ./terraform/main.tf. It provisions the following:

* GCS Buckets:

** <project-id>-datalake-raw: For raw data uploads.

** <project-id>-datalake-processed: For processed Parquet files.

** <project-id>-spark-scripts: To store the PySpark script.

** <project-id>-airflow-dags: Used by Cloud Composer to read DAG files (though Composer often creates its own).

* BigQuery Datasets:

** open_payments_staging: For raw data loaded from GCS.

** open_payments_analytics: For the final transformed table used by the dashboard.

* Dataproc Cluster: A cluster named etl-cluster-<region> for running Spark jobs.

* IAM: Permissions for the Dataproc service account (dataproc-sa@...) and the Composer/Airflow service account (de-project-service-account@...) to access GCS, BigQuery, and Dataproc resources.
