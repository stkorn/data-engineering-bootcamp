import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DoubleType

BUSINESS_DOMAIN = "greenery"
BUCKET_NAME = "deb6-bootcamp-17"
DATA = "orders"
KEYFILE_PATH = "/opt/spark/config/deb-upload-to-gcs.json"

# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
#     .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("transform_orders") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# SCHEMA = [
#     bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
#     bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
#     bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT),
#     bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT),
#     bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT),
#     bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
#     bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
#     bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
#     bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
#     bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
#     bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
#     bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
#     bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
# ]
# Example schema for Greenery users data
struct_schema = StructType([
    StructField("order_id", StringType()),
    StructField("created_at", TimestampType()),
    StructField("order_cost", DoubleType()),
    StructField("shipping_cost", DoubleType()),
    StructField("order_total", DoubleType()),
    StructField("tracking_id", StringType()),
    StructField("shipping_service", StringType()),
    StructField("estimated_delivery_at", TimestampType()),
    StructField("delivered_at", TimestampType()),
    StructField("status", StringType()),
    StructField("user_id", StringType()),
    StructField("promo_id", StringType()),
    StructField("address_id", StringType()),
])

execution_date = os.getenv("EXECUTION_DATE")
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{DATA}/{execution_date}/{DATA}.csv"

# df = spark.read \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .csv(GCS_FILE_PATH)

df = spark.read \
    .option("header", True) \
    .schema(struct_schema) \
    .csv(GCS_FILE_PATH)

df.show()

df.createOrReplaceTempView("items")
result = spark.sql("""
    select
        *

    from items
""")

OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/{execution_date}"
result.write.mode("overwrite").parquet(OUTPUT_PATH)