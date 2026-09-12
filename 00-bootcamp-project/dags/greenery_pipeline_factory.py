import csv
import json
from typing import Callable

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "gen-lang-client-0322103833"
DAGS_FOLDER = "/opt/airflow/dags"
BUCKET_NAME = "deb6-bootcamp-17"
BQ_DATASET = "deb_bootcamp"


def create_greenery_pipeline(
    data: str,
    header: list[str],
    row_mapper: Callable[[dict], list],
    schema: list[bigquery.SchemaField] | None = None,
    schedule: str = "@daily",
    start_date=timezone.datetime(2026, 8, 29),
    is_partition: bool = False,
) -> DAG:
    """Build extract -> GCS -> Spark transform -> BigQuery DAG for one Greenery dataset."""

    def _extract_data(ds=None):
        dataUrl = data.replace("_", "-")
        url = f"http://34.87.139.82:8000/{dataUrl}/"
        if is_partition:
            url += f"?created_at={ds}"
        response = requests.get(url)
        records = response.json()

        if records:

            raw_file_name = f"{data}-{ds}" if is_partition else data
            with open(f"{DAGS_FOLDER}/raw/{raw_file_name}.csv", "w") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                for record in records:
                    writer.writerow(row_mapper(record))

                # From this

                # for each in data:
                # data = [
                #     each["address_id"],
                #     each["address"],
                #     each["zipcode"],
                #     each["state"],
                #     each["country"],
                # ]
                # writer.writerow(data)
            return "load_data_to_gcs"

        else:
            return "do_nothing"

    def _load_data_to_gcs(ds=None):
        keyfile_gcs = "/opt/airflow/config/deb-upload-to-gcs.json"
        service_account_info_gcs = json.load(open(keyfile_gcs))
        credentials_gcs = service_account.Credentials.from_service_account_info(
            service_account_info_gcs
        )

        storage_client = storage.Client(
            project=GCP_PROJECT_ID,
            credentials=credentials_gcs,
        )
        bucket = storage_client.bucket(BUCKET_NAME)

        raw_file_name = f"{data}-{ds}" if is_partition else data
        file_path = f"{DAGS_FOLDER}/raw/{raw_file_name}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/"
        if is_partition:
            destination_blob_name += f"{ds}/"
        destination_blob_name += f"{data}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

    def _load_data_from_gcs_to_bigquery(ds=None):
        keyfile_bigquery = "/opt/airflow/config/deb-load-data-to-bigquery.json"
        service_account_info_bigquery = json.load(open(keyfile_bigquery))
        credentials_bigquery = service_account.Credentials.from_service_account_info(
            service_account_info_bigquery
        )

        bigquery_client = bigquery.Client(
            project=GCP_PROJECT_ID,
            credentials=credentials_bigquery,
            location=LOCATION,
        )

        partition = ds.replace("-", "")
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{data}${partition}" if is_partition else f"{GCP_PROJECT_ID}.{BQ_DATASET}.{data}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ) if is_partition else None,
        )

        destination_blob_name = f"cleaned/{BUSINESS_DOMAIN}/{data}/"
        if is_partition:
            destination_blob_name += f"{ds}/"
        destination_blob_name += "*.parquet"
        job = bigquery_client.load_table_from_uri(
            f"gs://{BUCKET_NAME}/{destination_blob_name}",
            table_id,
            job_config=job_config,
            location=LOCATION,
        )
        job.result()

        table = bigquery_client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    default_args = {
        "owner": "airflow",
        "start_date": start_date,
    }
    with DAG(
        dag_id=f"greenery_{data}_data_pipeline",
        default_args=default_args,
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        tags=["DEB", "Skooldio", BUSINESS_DOMAIN],
    ):

        extract_data = BranchPythonOperator(
            task_id="extract_data",
            python_callable=_extract_data,
        )

        load_data_to_gcs = PythonOperator(
            task_id="load_data_to_gcs",
            python_callable=_load_data_to_gcs,
        )

        transform_data = SparkSubmitOperator(
            task_id="transform_data",
            application=f"/opt/spark/pyspark/transform_{data}.py",
            conn_id="my_spark_conn",
            env_vars={"EXECUTION_DATE": "{{ ds }}"}
        )

        load_data_from_gcs_to_bigquery = PythonOperator(
            task_id="load_data_from_gcs_to_bigquery",
            python_callable=_load_data_from_gcs_to_bigquery,
        )

        do_nothing = EmptyOperator(task_id="do_nothing")

        end = EmptyOperator(task_id="end", trigger_rule="one_success")

        extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery >> end
        extract_data >> do_nothing >> end  # If no records, skip to end
