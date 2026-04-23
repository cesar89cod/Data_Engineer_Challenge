from airflow import DAG
from airflow.operators.python import PythonOperator
from trino.dbapi import connect
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

def validate_landing_and_prepare_bronze():
    s3 = get_s3_client()

    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]

    # 1. Validar bucket landing
    if LANDING_BUCKET not in buckets:
        raise ValueError(f"Bucket requerido no existe: {LANDING_BUCKET}")

    # 2. Validar que el archivo exista en landing
    try:
        s3.head_object(Bucket=LANDING_BUCKET, Key=CSV_KEY)
    except Exception:
        raise ValueError(
            f"Archivo requerido no encontrado en MinIO: "
            f"s3://{LANDING_BUCKET}/{CSV_KEY}"
        )

    # 3. Crear bucket bronze si no existe
    if BRONZE_BUCKET not in buckets:
        s3.create_bucket(Bucket=BRONZE_BUCKET)


def read_csv_from_minio():
    s3 = get_s3_client()
    s3.download_file(LANDING_BUCKET, CSV_KEY, LOCAL_TMP)


def transform_and_aggregate():
    df = pl.read_csv(LOCAL_TMP)

    # Limpieza básica
    df = df.with_columns(
        [
            pl.col("name").str.strip_chars().str.to_lowercase(),
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
    
def create_trino_objects():
    conn = connect(
        host="trino",
        port=8080,
        user="airflow",
        catalog="bronze",
        schema="default",
    )
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS bronze.prueba")

    cur.execute("""
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
    """)


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="etl_engineer_challenge",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "minio", "polars", "trino"],
) as dag:


    t1 = PythonOperator(
        task_id="validate_landing_and_prepare_bronze",
        python_callable=validate_landing_and_prepare_bronze,
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

    t5 = PythonOperator(
    task_id="create_trino_schema_and_table",
    python_callable=create_trino_objects,
    )

    t1 >> t2 >> t3 >> t4 >> t5