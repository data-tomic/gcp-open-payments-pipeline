# open_payments_dag.py
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# --- Конфигурационные переменные --- #

# GCP Project ID и Region (должны совпадать с Terraform)
GCP_PROJECT_ID = "original-glider-455309-s7"
GCP_REGION = "us-central1"

# Имена GCS бакетов (должны совпадать с Terraform)
GCS_RAW_BUCKET = f"{GCP_PROJECT_ID}-datalake-raw"
GCS_PROCESSED_BUCKET = f"{GCP_PROJECT_ID}-datalake-processed"
GCS_SPARK_SCRIPTS_BUCKET = f"{GCP_PROJECT_ID}-spark-scripts"
GCS_DAGS_BUCKET = f"{GCP_PROJECT_ID}-airflow-dags" # Добавим для полноты

# Пути ввода/вывода
# --- ВНИМАНИЕ: Проверьте этот путь ---
# Если у вас один файл CSV:
# GCS_RAW_INPUT_PATH = f"gs://{GCS_RAW_BUCKET}/open_payments/ВАШ_ФАЙЛ.csv"
# Если Spark должен читать все из папки:
GCS_RAW_INPUT_PATH = f"gs://{GCS_RAW_BUCKET}/open_payments/unzipped/"
GCS_PROCESSED_OUTPUT_PATH = f"gs://{GCS_PROCESSED_BUCKET}/open_payments_parquet/"
GCS_SPARK_SCRIPT_PATH = f"gs://{GCS_SPARK_SCRIPTS_BUCKET}/process_payments.py"

# Имена BigQuery Dataset и Таблиц (должны совпадать с Terraform)
BQ_STAGING_DATASET = "open_payments_staging"
BQ_ANALYTICS_DATASET = "open_payments_analytics"
BQ_STAGING_TABLE = "raw_payments"
BQ_ANALYTICS_TABLE = "payments_reporting"
BQ_LOCATION = "US" # Должно совпадать с локацией датасетов BQ

# Имя кластера Dataproc (должно совпадать с Terraform)
DATAPROC_CLUSTER_NAME = f"etl-cluster-{GCP_REGION}"

# SQL для трансформации (оставляем как есть или выносим в .sql файл)
TRANSFORM_SQL = f"""
CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.{BQ_ANALYTICS_TABLE}`
PARTITION BY payment_date -- Опционально: Партиционирование по дате
CLUSTER BY recipient_state, payment_nature -- Опционально: Кластеризация
OPTIONS (
     description="Cleaned and aggregated open payments data for reporting"
     -- labels=[("source", "openpayments"), ("state", "curated")] -- Пример меток
) AS
SELECT
    Record_ID,
    recipient_npi,
    recipient_first_name,
    recipient_last_name,
    recipient_city,
    recipient_state,
    physician_specialty,
    payer_name,
    payment_form,
    payment_nature,
    program_year,
    -- Безопасное преобразование строки в дату (формат ММ/ДД/ГГГГ)
    SAFE.PARSE_DATE('%m/%d/%Y', payment_date_str) AS payment_date,
    -- Безопасное преобразование суммы в NUMERIC
    SAFE_CAST(payment_amount_usd AS NUMERIC) AS payment_amount_usd
FROM
    `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
WHERE
    -- Обязательно добавляем фильтр, чтобы исключить строки, где дата не распарсилась
    SAFE.PARSE_DATE('%m/%d/%Y', payment_date_str) IS NOT NULL
    -- и где сумма не является числом (или NULL)
    AND SAFE_CAST(payment_amount_usd AS NUMERIC) IS NOT NULL;
"""

# --- Определение DAG --- #

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1), # Установите дату начала в прошлом
}

with DAG(
    dag_id='open_payments_pipeline_v2', # Изменил версию для наглядности
    default_args=default_args,
    description='End-to-end pipeline for CMS Open Payments data',
    schedule_interval='@daily',  # Запуск ежедневно, измените на None для ручного запуска
    catchup=False,
    tags=['gcp', 'dataproc', 'bigquery', 'openpayments', 'final-project'],
    # Указываем сервис аккаунт для Airflow (если используется Composer, он обычно настраивается при создании среды)
    # default_view='graph', # Можно установить вид по умолчанию
) as dag:

    submit_spark_job = DataprocSubmitPySparkJobOperator(
        task_id='submit_spark_processing_job',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        main=GCS_SPARK_SCRIPT_PATH,
        # Передаем пути как аргументы скрипту
        arguments=[
            f"--input_path={GCS_RAW_INPUT_PATH}",
            f"--output_path={GCS_PROCESSED_OUTPUT_PATH}"
        ],
        job_name='open-payments-spark-{{ ds_nodash }}', # Уникальное имя задачи
        # Указываем gcp_conn_id если нужно использовать не 'google_cloud_default'
        # gcp_conn_id = 'my_gcp_connection'
    )

    load_processed_to_staging = GCSToBigQueryOperator(
        task_id='load_processed_parquet_to_bq_staging',
        bucket=GCS_PROCESSED_BUCKET,
        # Используем wildcard для чтения всех parquet файлов из папки
        source_objects=[f"open_payments_parquet/*.parquet"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE', # Перезаписываем стейджинг таблицу
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True, # BQ определит схему из Parquet
        location=BQ_LOCATION, # Явно указываем локацию BQ
        # gcp_conn_id = 'my_gcp_connection'
    )

    transform_in_bq = BigQueryExecuteQueryOperator(
        task_id='transform_staging_to_analytics',
        sql=TRANSFORM_SQL,
        use_legacy_sql=False,
        # write_disposition='WRITE_TRUNCATE', # CREATE OR REPLACE TABLE уже включает перезапись
        allow_large_results=True, # Разрешаем большие результаты
        location=BQ_LOCATION, # Явно указываем локацию BQ
        # gcp_conn_id = 'my_gcp_connection'
    )

    # Определяем зависимости задач
    submit_spark_job >> load_processed_to_staging >> transform_in_bq
