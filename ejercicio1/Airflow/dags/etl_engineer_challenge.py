from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime
import polars as pl
import boto3
import os

# -----------------------
# CONFIGURACIÓN
# -----------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"

LANDING_BUCKET = "bck-landing"
BRONZE_BUCKET = "bck-bronze"

CSV_KEY = "data/data_prueba_tecnica.csv"
PARQUET_KEY = "master/data_prueba_tecnica.parquet"

LOCAL_TMP = "/tmp/data_prueba_tecnica.csv"
LOCAL_PARQUET = "/tmp/data_prueba_tecnica.parquet"


# -----------------------
# CLIENTE MINIO (S3)
# -----------------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# -----------------------
# TASKS
# -----------------------

def ensure_buckets():
    s3 = get_s3_client()
    for bucket in [LANDING_BUCKET, BRONZE_BUCKET]:
        existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)


def read_csv_from_minio():
    s3 = get_s3_client()
    s3.download_file(LANDING_BUCKET, CSV_KEY, LOCAL_TMP)


def transform_and_aggregate():
    df = pl.read_csv(LOCAL_TMP)

    # Limpieza básica
    df = df.with_columns(
        [
            pl.col("name").str.strip().str.to_lowercase(),
            pl.col("created_at").str.strptime(pl.Datetime, strict=False),
        ]
    ).drop_nulls()

    # Agregaciones
    result = (
        df.group_by("name")
        .agg(
            [
                pl.count().alias("total_records"),
                pl.col("created_at").min().alias("first_created_at"),
                pl.col("created_at").max().alias("last_created_at"),
            ]
        )
    )

    result.write_parquet(LOCAL_PARQUET)


def write_parquet_to_minio():
    s3 = get_s3_client()
    s3.upload_file(LOCAL_PARQUET, BRONZE_BUCKET, PARQUET_KEY)


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="etl_engineer_challenge",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "minio", "polars", "trino"],
) as dag:

    t1 = PythonOperator(
        task_id="ensure_buckets",
        python_callable=ensure_buckets,
    )

    t2 = PythonOperator(
        task_id="read_csv_from_minio",
        python_callable=read_csv_from_minio,
    )

    t3 = PythonOperator(
        task_id="transform_and_aggregate",
        python_callable=transform_and_aggregate,
    )

    t4 = PythonOperator(
        task_id="write_parquet_to_minio",
        python_callable=write_parquet_to_minio,
    )

    t5 = TrinoOperator(
        task_id="create_schema_trino",
        sql="CREATE SCHEMA IF NOT EXISTS bronze.prueba",
        trino_conn_id="trino_default",
    )

    t6 = TrinoOperator(
        task_id="create_table_trino",
        sql="""
        CREATE TABLE IF NOT EXISTS bronze.prueba.tbl_data (
            name VARCHAR,
            total_records INTEGER,
            first_created_at TIMESTAMP,
            last_created_at TIMESTAMP
        )
        WITH (
            external_location = 's3://bck-bronze/master/',
            format = 'PARQUET'
        )
        """,
        trino_conn_id="trino_default",
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6