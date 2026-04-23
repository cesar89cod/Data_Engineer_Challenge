
# ======================================================
# DAG: ETL_ENGINEER_CHALLENGE
# Autor      : César Hernández Hernández
# Creación   : 2026-04-23
# Versión    : 1.0
# ======================================================
# Propósito:
#   Este DAG implementa un pipeline ETL end-to-end que
#   ingesta datos transaccionales desde un archivo CSV,
#   aplica procesos de limpieza y agregación, los
#   almacena en formato Parquet sobre un Data Lake
#   (MinIO) y los expone mediante tablas externas
#   consultables desde Trino.
# ======================================================

from airflow import DAG                              # DAG: define el flujo de trabajo en Airflow.
from airflow.operators.python import PythonOperator  # PythonOperator: permite ejecutar funciones Python como tasks.
from airflow.operators.dummy import DummyOperator    # Dummy Operator: start an end task.
from trino.dbapi import connect                      # Cliente DB‑API para ejecutar SQL en Trino.
from datetime import datetime, timedelta             # datetime: define la fecha de inicio del DAG.
import polars as pl                                  # polars: librería de procesamiento de datos.
import boto3                                         # boto3: cliente S3 compatible con MinIO.
import os                                            # os: utilidades del sistema operativo.

##############################################
# DAG CONFIGURATION REQUIRED
##############################################

WORKFLOW = 'etl_engineer_challenge'
TAGS = ['etl', 'minio', 'polars', 'trino']
SLA_MINS = 10
SCHEDULE = None

#Configuración de conexión a MinIO (servicio S3‑compatible).
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"

#Buckets que representan las zonas del Data Lake:
LANDING_BUCKET = "bck-landing" #- landing: datos crudos
BRONZE_BUCKET = "bck-bronze"   #- bronze: datos procesados

#Rutas lógicas (keys) de los objetos dentro de MinIO.
CSV_KEY = "data/data_prueba_tecnica.csv"
PARQUET_KEY = "master/data_prueba_tecnica.parquet"

#Rutas temporales locales dentro del contenedor de Airflow para procesar los datos.
LOCAL_TMP = "/tmp/data_prueba_tecnica.csv"
LOCAL_PARQUET = "/tmp/data_prueba_tecnica.parquet"


##############################################
# CLIENTE MINIO (S3)
##############################################

#Esta función crea un cliente S3 reutilizable para conectarse a MinIO.
#Se utiliza en todos los tasks que interactúan con almacenamiento.
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


##############################################
# TASKS DEL ETL
##############################################

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


#Descarga el archivo CSV desde el bucket bck-landing y lo guarda temporalmente en el contenedor de Airflow.
#Separa claramente la capa de almacenamiento de la de procesamiento.

def read_csv_from_minio():
    s3 = get_s3_client()
    s3.download_file(LANDING_BUCKET, CSV_KEY, LOCAL_TMP)


#Lee el CSV usando Polars.

#Limpieza de datos:
#- Elimina espacios y normaliza el campo name a minúsculas.
#- Convierte created_at a tipo datetime.
#- Elimina registros con valores nulos.

#Transformaciones y agregaciones:
#- Agrupa por name.
#- Calcula:
#  - total de registros
#  - primera fecha de creación
#  - última fecha de creación

#El resultado se guarda localmente en formato Parquet.
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

#Sube el archivo Parquet procesado al bucket bck-bronze,en la ruta master/data_prueba_tecnica.parquet.
#Esto representa la zona bronze del Data Lake.
def write_parquet_to_minio():
    s3 = get_s3_client()
    s3.upload_file(LOCAL_PARQUET, BRONZE_BUCKET, PARQUET_KEY)

#Se conecta a Trino usando el cliente DB‑API desde Python.
#Crea:
#- El schema bronze.prueba (si no existe).
#- La tabla externa bronze.prueba.tbl_data.
#La tabla lee directamente los archivos Parquet almacenados en MinIO,permitiendo consultas SQL sin mover los datos ya que es una external table.    
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
            external_location = 's3a://bck-bronze/master/',
            format = 'PARQUET'
        )
    """)


##############################################
# Default DAG arguments & configuration
##############################################
#Create the DAG
with DAG(
    workflow,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'sla': timedelta(minutes=int(SLA_MINS))},
    description=workflow,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    tags=TAGS,
    max_active_runs=1
) as dag:

##############################################
# Define workflow tasks
##############################################

    start = DummyOperator(
        task_id="start",
    )

    #1. Creación/verificación de buckets y existencia del file.
    t1 = PythonOperator(
        task_id="validate_landing_and_prepare_bronze",
        python_callable=validate_landing_and_prepare_bronze,
    )

    #2. Lectura del CSV desde MinIO.
    t2 = PythonOperator(
        task_id="read_csv_from_minio",
        python_callable=read_csv_from_minio,
    )
    
    #3. Limpieza, transformación y agregación con Polars.
    t3 = PythonOperator(
        task_id="transform_and_aggregate",
        python_callable=transform_and_aggregate,
    )
    
    #4. Escritura del Parquet en la zona bronze.
    t4 = PythonOperator(
        task_id="write_parquet_to_minio",
        python_callable=write_parquet_to_minio,
    )
    
    #5. Habilitación de la información para consulta en Trino.
    t5 = PythonOperator(
    task_id="create_trino_schema_and_table",
    python_callable=create_trino_objects,
    )
    
    
    end = DummyOperator(
        task_id="end",
    )


    start >> t1 >> t2 >> t3 >> t4 >> t5 >> end
    