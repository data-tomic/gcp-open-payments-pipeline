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

### Architecture Diagram

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
``` 


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
The infrastructure is defined in [`./terraform/main.tf`](./terraform/main.tf). It provisions the following GCP resources:

*   **GCS Buckets:**
    *   `<project-id>-datalake-raw`: Stores the initial raw CSV data uploads (Bronze Layer). Lifecycle rule configured to delete objects older than 30 days (configurable).
    *   `<project-id>-datalake-processed`: Stores the processed data in Parquet format generated by Spark (Silver Layer).
    *   `<project-id>-spark-scripts`: Used to store the PySpark processing script (`process_payments.py`) for Dataproc access.
    *   `<project-id>-airflow-dags`: Intended for Airflow DAGs. *Note: Cloud Composer typically creates and uses its own dedicated bucket for DAGs, which should be used in practice.*

*   **BigQuery Datasets:**
    *   `open_payments_staging`: Dataset for the staging table (`raw_payments`) loaded directly from the processed GCS Parquet files. Configured with `delete_contents_on_destroy = true` for development ease.
    *   `open_payments_analytics`: Dataset for the final analytics table (`payments_reporting`) used by Looker Studio (Gold Layer). Also configured with `delete_contents_on_destroy = true`.

*   **Dataproc Cluster:**
    *   A cluster named `etl-cluster-<region>` (e.g., `etl-cluster-us-central1`) is created for running the PySpark job.
    *   Uses standard machine types (configurable in `main.tf`).
    *   Configured to run using the `dataproc-sa@...` service account.

*   **IAM Permissions:**
    *   **`dataproc-sa@...` Service Account:** Granted necessary roles (`roles/storage.objectViewer` on raw bucket, `roles/storage.objectCreator` on processed bucket, `roles/bigquery.dataEditor` on staging dataset, `roles/dataproc.worker`) to allow cluster nodes to read from GCS, write to GCS, write to the BigQuery staging table, and operate as Dataproc workers.
    *   **`de-project-service-account@...` Service Account:** This account is used by Cloud Composer/Airflow. It's granted roles (`roles/storage.objectAdmin` on the DAGs bucket (or the Composer bucket), `roles/storage.objectViewer` on the Spark scripts bucket, `roles/dataproc.editor` to submit jobs, `roles/bigquery.dataEditor` on staging and analytics datasets, `roles/bigquery.jobUser` to run BQ jobs, and the essential `roles/composer.worker` for Composer operations) allowing it to manage DAGs, read scripts, submit Dataproc jobs, load data into BigQuery, run transformation queries, and interact with the Composer environment.

*(Link to main Terraform code: [`./terraform/main.tf`](./terraform/main.tf))*
*(See the file for detailed resource configurations and variable definitions)*

### 2. Data Processing (Spark)

The PySpark script [`./spark/process_payments.py`](./spark/process_payments.py) handles the initial Extract, Transform, Load (ETL) phase from the raw data lake layer to the processed layer:

*   **Schema Definition:** A `StructType` schema is explicitly defined based on the known columns of the `OP_DTL_GNRL_PGYR2023_P01302025.csv` file. This ensures data types are correctly inferred (especially for amounts and potentially problematic fields) and improves the performance and reliability of reading the large CSV file compared to `inferSchema=true`.
*   **Reading Data:** The script reads the specific CSV file from the `gs://<project-id>-datalake-raw/open_payments/unzipped/` path using `spark.read.csv()`, applying the defined schema and options like `header=True`, `multiLine=True`, and `escape='"'` to handle potential complexities in the data.
*   **Transformation:**
    *   **Column Selection:** Only columns deemed necessary for the final analytics table and potential future use are selected using `.select()`.
    *   **Renaming:** Key columns are renamed using `.alias()` for better readability and consistency (e.g., `Total_Amount_of_Payment_USDollars` becomes `payment_amount_usd`, `Covered_Recipient_Specialty_1` becomes `physician_specialty`).
    *   **Basic Filtering:** A simple filter (`.filter(F.col("Record_ID").isNotNull())`) is applied to remove any potential records lacking a primary identifier.
*   **Writing Data:** The processed DataFrame is written to the processed GCS bucket (`gs://<project-id>-datalake-processed/open_payments_parquet/`) in **Parquet** format.
    *   **Format Choice:** Parquet is chosen for its efficient columnar storage, compression capabilities, and excellent integration with downstream tools like BigQuery and Spark.
    *   **Write Mode:** `mode="overwrite"` is used to ensure that each run of the pipeline replaces the previous processed data, suitable for a batch workflow processing a full dataset snapshot.

*(Link to PySpark code: [`./spark/process_payments.py`](./spark/process_payments.py))*

---

### 3. Orchestration (Airflow)

The entire pipeline workflow is orchestrated using Apache Airflow, managed via **Google Cloud Composer 2**. The Directed Acyclic Graph (DAG) definition is located in [`./airflow/dags/open_payments_dag.py`](./airflow/dags/open_payments_dag.py).

*   **DAG Schedule:** Configured to run daily (`schedule_interval='@daily'`) but primarily triggered manually for this project. `catchup=False` prevents backfilling for past missed schedules.
*   **Operators Used:**
    1.  `DataprocSubmitPySparkJobOperator` (`submit_spark_processing_job` task): Submits the `process_payments.py` script to the provisioned Dataproc cluster, passing the GCS input and output paths as arguments.
    2.  `GCSToBigQueryOperator` (`load_processed_parquet_to_bq_staging` task): Loads the Parquet files generated by the Spark job (using wildcard `*.parquet`) from the processed GCS bucket into the BigQuery staging table (`open_payments_staging.raw_payments`). It uses `autodetect=True` for schema detection from Parquet and `write_disposition='WRITE_TRUNCATE'` to replace the table content on each run.
    3.  `BigQueryExecuteQueryOperator` (`transform_in_bq` task): Executes the final SQL transformation query directly within BigQuery to populate the analytics table (`open_payments_analytics.payments_reporting`) from the staging table. The SQL query is embedded within the DAG file for simplicity in this project.
*   **Task Dependencies:** The tasks are linked sequentially: `submit_spark_processing_job` >> `load_processed_parquet_to_bq_staging` >> `transform_in_bq`.
*   **Configuration:** Variables at the top of the DAG file define GCP project ID, region, bucket names, BigQuery dataset/table names, and Dataproc cluster name, ensuring consistency with the Terraform setup.

*(Link to Airflow DAG code: [`./airflow/dags/open_payments_dag.py`](./airflow/dags/open_payments_dag.py))*

---

### 4. Data Warehousing & Transformation (BigQuery)

Google BigQuery serves as the data warehouse for this project.

*   **Staging Layer:**
    *   Dataset: `open_payments_staging`
    *   Table: `raw_payments`
    *   Purpose: Holds the intermediate data loaded directly from the processed Parquet files in GCS. This table structure mirrors the Parquet schema. It is overwritten on each pipeline run.

*   **Analytics Layer (Gold Layer):**
    *   Dataset: `open_payments_analytics`
    *   Table: `payments_reporting`
    *   Purpose: This is the final, cleaned, and optimized table designed for consumption by the BI tool (Looker Studio).
    *   **Transformation Logic (SQL):** The `BigQueryExecuteQueryOperator` runs a `CREATE OR REPLACE TABLE ... AS SELECT ...` statement. Key transformations include:
        *   Selecting specific columns required for the dashboard (`Record_ID`, `recipient_npi`, `recipient_first_name`, `recipient_last_name`, `recipient_city`, `recipient_state`, `physician_specialty`, `payer_name`, `payment_form`, `payment_nature`, `program_year`, `payment_date_str`, `payment_amount_usd`).
        *   **Data Type Conversion & Cleaning:** Using `SAFE.PARSE_DATE('%m/%d/%Y', payment_date_str)` to convert the payment date string into a proper `DATE` type, safely handling potential parsing errors (returning NULL instead of failing). Using `SAFE_CAST(payment_amount_usd AS NUMERIC)` to convert the payment amount into a `NUMERIC` type, also handling errors safely.
        *   **Filtering:** The `WHERE` clause (`SAFE.PARSE_DATE(...) IS NOT NULL AND SAFE_CAST(...) IS NOT NULL`) ensures that only records with valid, parseable dates and amounts are included in the final analytics table, improving data quality for visualization.
    *   **Table Optimization:**
        *   **Partitioning:** The table is partitioned by the `payment_date` column (`PARTITION BY payment_date`). Since dashboards often allow filtering by date ranges (e.g., last month, specific quarter, year), partitioning ensures that BigQuery only scans the necessary partitions (days, in this case) relevant to the query's date filter, significantly reducing query cost and improving performance.
        *   **Clustering:** The table is clustered by `recipient_state` and `payment_nature` (`CLUSTER BY recipient_state, payment_nature`). These columns are used as dimensions in the dashboard's categorical visualizations. Clustering physically co-locates data rows with the same state or payment nature within each partition, which speeds up queries that filter or aggregate on these columns (e.g., `WHERE recipient_state = 'CA'` or `GROUP BY payment_nature`).

*(Link to Transformation SQL (embedded in DAG): [`./airflow/dags/open_payments_dag.py`](./airflow/dags/open_payments_dag.py) - see `TRANSFORM_SQL` variable)*
*(Or if externalized: [`./sql/transform_payments.sql`](./sql/transform_payments.sql))*

---

### 5. Dashboard

A dashboard was created using **Looker Studio** to visualize the insights from the processed data.

*   **Data Source:** The dashboard connects directly to the final analytics table: `original-glider-455309-s7.open_payments_analytics.payments_reporting` in BigQuery.
*   **Visualizations:**
    1.  **Categorical Distribution:** A bar chart titled "Payment Count by Nature" displays the number of payment records (`Record Count`) grouped by the `payment_nature` dimension.
    2.  **Temporal Distribution:** A time-series chart titled "Total Payment Amount Over Time" displays the sum of `payment_amount_usd` aggregated by month, using `payment_date` as the time dimension.
*   **Interactivity:** A **Date Range Control** filter is included and recommended to be set to **Jan 1, 2023 - Dec 31, 2023** to view the data relevant to Program Year 2023, taking advantage of the table partitioning.
*   **Note:** The dashboard only reflects data for **Program Year 2023**.

**Dashboard Screenshot:**
![Looker Studio Dashboard](images/dashboard.png) 

**(Link to Dashboard)**
[https://lookerstudio.google.com/reporting/eb3fb8f6-6a85-467c-925d-f1b8ba35ad9f/page/DuKFF](https://lookerstudio.google.com/reporting/eb3fb8f6-6a85-467c-925d-f1b8ba35ad9f/page/DuKFF)

## How to Reproduce

Follow these steps to set up the infrastructure and run the pipeline in your own GCP environment.

**Prerequisites:**

*   **GCP Account:** A Google Cloud Platform account with active billing enabled.
*   **IAM Permissions:** Your Google user account needs permissions to create and manage GCP resources, including Compute Engine, GCS, BigQuery, Dataproc, Cloud Composer, and IAM roles. The `Owner` role on the project is the simplest way to ensure this during setup.
*   **Enabled APIs:** Ensure the following APIs are enabled in your GCP project: Compute Engine API, Cloud Storage API, BigQuery API, Dataproc API, Cloud Composer API, IAM API, Cloud Resource Manager API. You can enable them via the GCP Console under "APIs & Services".
*   **`gcloud` CLI:** Google Cloud SDK installed and authenticated locally.
    *   Authenticate your user account: `gcloud auth login`
    *   Set up Application Default Credentials: `gcloud auth application-default login`
*   **Terraform CLI:** Terraform (version >= 1.0 recommended) installed locally. [Install Guide](https://learn.hashicorp.com/tutorials/terraform/install-cli)
*   **Git:** Git installed locally.
*   **Google Account:** For accessing the Cloud Composer Airflow UI and Looker Studio.

**Steps:**

1.  **Clone Repository:**
    Clone this repository to your local machine:
    ```bash
    git clone https://github.com/data-tomic/gcp-open-payments-pipeline.git # Use your repo URL if different
    cd gcp-open-payments-pipeline
    ```

2.  **Configure GCP Project:**
    *   Set your target GCP Project ID as an environment variable (replace `"your-gcp-project-id"`):
        ```bash
        export GCP_PROJECT_ID="your-gcp-project-id"
        ```
    *   Configure the `gcloud` CLI to use this project for subsequent commands:
        ```bash
        gcloud config set project $GCP_PROJECT_ID
        ```
    *   *(Note: Terraform configuration in `/terraform` will also use this project ID, either via its default variable or passed via command line).*

3.  **Provision Infrastructure with Terraform:**
    *   Navigate to the Terraform configuration directory:
        ```bash
        cd terraform
        ```
    *   Initialize Terraform to download provider plugins:
        ```bash
        terraform init
        ```
    *   *(Optional)* Preview the resources Terraform will create:
        ```bash
        terraform plan -var="project_id=$GCP_PROJECT_ID"
        ```
    *   Apply the Terraform configuration to create GCP resources:
        ```bash
        terraform apply -var="project_id=$GCP_PROJECT_ID"
        ```
        Review the planned changes and type `yes` when prompted. This step provisions GCS buckets (e.g., `${GCP_PROJECT_ID}-datalake-raw`), BigQuery datasets (`open_payments_staging`, `open_payments_analytics`), a Dataproc cluster (`etl-cluster-...`), and configures IAM roles for the necessary service accounts. Wait for completion.

4.  **Download and Prepare Data:**
    *   Download the Open Payments Program Year 2023 General Payments zip file:
        [https://download.cms.gov/openpayments/PGYR2023_P01302025_01212025.zip](https://download.cms.gov/openpayments/PGYR2023_P01302025_01212025.zip)
    *   Unzip the downloaded file.
    *   Identify the main data file, likely named `OP_DTL_GNRL_PGYR2023_P01302025.csv`.

5.  **Upload Raw Data to GCS Data Lake:**
    *   Define the raw data bucket name variable:
        ```bash
        RAW_BUCKET_NAME="${GCP_PROJECT_ID}-datalake-raw"
        ```
    *   Upload the CSV file to the specific `unzipped` folder within the raw bucket (replace `<path-to-your-csv-file>`):
        ```bash
        gsutil cp <path-to-your-csv-file>/OP_DTL_GNRL_PGYR2023_P01302025.csv gs://${RAW_BUCKET_NAME}/open_payments/unzipped/
        ```

6.  **Create Cloud Composer Environment:**
    *   In the GCP Console, navigate to "Composer".
    *   Click **"Create Environment"**. Choose **Composer 2**.
    *   Enter an **Environment name** (e.g., `open-payments-airflow`). Use lowercase letters, numbers, hyphens.
    *   Select the same **Location (Region)** used in your Terraform configuration (e.g., `us-central1`).
    *   Select an appropriate **Image version**.
    *   **Crucial:** Under **Service Account**, select the custom service account created by Terraform: **`de-project-service-account@${GCP_PROJECT_ID}.iam.gserviceaccount.com`**. Do *not* use the default Compute Engine service account.
    *   Configure node sizes/counts if needed, otherwise use defaults.
    *   Click **"Create"**. Wait patiently (20-30+ minutes).

7.  **Deploy Pipeline Code (Scripts & DAG):**
    *   Once Composer is ready, go to its environment details page in the GCP Console. Find and copy the **"DAGs folder" GCS bucket name** (e.g., `us-central1-open-payments-...-bucket`).
    *   Set variables for bucket names:
        ```bash
        # Replace with the actual Composer DAGs bucket name copied from the console
        COMPOSER_DAGS_BUCKET="your-composer-dags-bucket-name"
        SPARK_SCRIPTS_BUCKET_NAME="${GCP_PROJECT_ID}-spark-scripts"
        ```
    *   Navigate back to the **root directory** of your cloned project locally.
    *   Upload the PySpark script to its GCS bucket:
        ```bash
        gsutil cp spark/process_payments.py gs://${SPARK_SCRIPTS_BUCKET_NAME}/
        ```
    *   Upload the Airflow DAG to the Composer DAGs bucket:
        ```bash
        gsutil cp airflow/dags/open_payments_dag.py gs://${COMPOSER_DAGS_BUCKET}/dags/
        ```
    *   *(Verification)* Ensure paths and resource names within `airflow/dags/open_payments_dag.py` are correct for your environment.

8.  **Execute the Airflow Pipeline:**
    *   From the Composer environment page, click the **"Airflow UI"** link.
    *   Locate the `open_payments_pipeline_v2` DAG. Wait for it to appear and check for import errors.
    *   **Unpause** the DAG using the toggle switch on the left if it's paused.
    *   Manually **Trigger** the DAG using the Play button (▶️) on the right.
    *   Monitor the execution in the Airflow UI (Grid/Graph view). Wait for all tasks (`submit_spark...`, `load_processed...`, `transform_in_bq`) to reach a **success** state. Use task logs for troubleshooting if needed.

9.  **Verify Data in BigQuery Warehouse:**
    *   In the GCP Console, navigate to BigQuery.
    *   Query the final analytics table to confirm data presence:
        ```sql
        SELECT COUNT(*)
        FROM `your-gcp-project-id.open_payments_analytics.payments_reporting`; -- Replace project ID
        ```
        The result should be a large number (e.g., ~14.6 million). You can also preview the table data.

10. **Set Up Looker Studio Dashboard:**
    *   Go to [Looker Studio](https://lookerstudio.google.com/).
    *   Create a **New Report** and **Add Data**.
    *   Select the **BigQuery** connector. Authorize if prompted.
    *   Navigate to **Your Project** -> `open_payments_analytics` -> `payments_reporting` table and click **Add**.
    *   Build the required dashboard tiles:
        *   **Categorical:** Bar Chart (Dimension: `payment_nature`, Metric: `Record Count`).
        *   **Temporal:** Time Series Chart (Time Dimension: `payment_date`, Metric: `payment_amount_usd`).
    *   Add a **Date Range Control** and set its default range to **Jan 1, 2023 - Dec 31, 2023** for accurate viewing.

11. **Clean Up Resources (Optional but Recommended):**
    *   To avoid incurring further GCP costs, destroy the infrastructure created by Terraform:
        ```bash
        # Navigate back to the terraform directory
        cd ../terraform
        terraform destroy -var="project_id=$GCP_PROJECT_ID"
        ```
        Type `yes` to confirm. This removes the Dataproc cluster, BigQuery datasets/tables, GCS buckets, etc.
    *   Manually **delete the Cloud Composer environment** from the GCP Console, as Terraform does not manage it in this setup.

## Evaluation Criteria Checklist

This project aims to meet the requirements outlined in the Data Engineering Zoomcamp criteria:

*   ✅ **Problem description:** The problem statement (creating a pipeline and dashboard for Open Payments data) is clearly described.
*   ✅ **Cloud:** Google Cloud Platform (GCP) is used for all components. Infrastructure is managed using Terraform (IaC). Code is included in the repository.
*   ✅ **Data ingestion (Batch):** An end-to-end batch pipeline is implemented. Workflow orchestration is handled by Airflow (via Cloud Composer), managing multiple dependent steps (Spark job, GCS->BQ load, BQ SQL transform). Raw data is ingested into a GCS Data Lake bucket.
*   ✅ **Data warehouse:** Google BigQuery is utilized as the Data Warehouse. The final analytics table (`payments_reporting`) is explicitly **partitioned** by `payment_date` to optimize time-based queries common in dashboards. It is also **clustered** by `recipient_state` and `payment_nature` to improve performance for filtering and aggregation based on these categorical dimensions used in the dashboard tiles.
*   ✅ **Transformations:** Transformations are implemented using both **PySpark** (for initial cleaning, schema enforcement, and conversion to Parquet) and **BigQuery SQL** (for final data type casting, cleaning, and structuring for the analytics layer). Code for both is included/referenced.
*   ✅ **Dashboard:** A dashboard with the two required tile types (categorical distribution and temporal distribution) was created using **Looker Studio**. A screenshot is provided, and explanations of the tiles are included.
*   ✅ **Reproducibility:** Detailed, step-by-step instructions are provided in the "How to Reproduce" section. All necessary code (`.tf`, `.py`, DAG) and configuration details are included in the repository, making it possible for others to run the pipeline.

## Future Improvements (Optional)

While the core requirements are met, the following enhancements could be considered for a production-grade system or portfolio piece:

*   **Testing:**
    *   Implement unit tests for the PySpark data transformation logic using a framework like `pytest` and potentially `chispa`.
    *   Add data quality checks within the Airflow DAG (e.g., using `SQLCheckOperator`, custom Python operators, or integrating with tools like Great Expectations) to validate data after loading or transformation steps.
*   **Automation & CI/CD:**
    *   Create a `Makefile` to simplify common commands (`terraform apply`, `terraform destroy`, `upload_dag`, etc.).
    *   Set up a CI/CD pipeline (e.g., using GitHub Actions or Google Cloud Build) to automatically lint code, run tests, deploy Terraform changes, and update Airflow DAGs/scripts in GCS upon commits to the `main` branch.
*   **Parameterization:**
    *   Make the Airflow DAG more dynamic by using Airflow Variables or configuration files (`.yml`) to manage environment-specific settings (bucket names, project IDs, file paths, processing dates/years) instead of hardcoding them.
*   **Error Handling & Monitoring:**
    *   Configure more robust alerting in Airflow for task failures (e.g., Slack or email notifications).
    *   Integrate with Google Cloud Monitoring for pipeline metrics (e.g., job durations, data volumes processed).
*   **Schema Management:** For more complex scenarios, consider using a schema registry or defining schemas more formally (e.g., Avro schemas).
*   **Security Enhancements:** Refine IAM permissions to follow the principle of least privilege more strictly, potentially using custom roles.

---
*This project was developed as part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by DataTalks.Club.*
