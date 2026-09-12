import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

BUSINESS_DOMAIN = "greenery"
BUCKET_NAME = "deb6-bootcamp-17"
DATA = "events"
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

execution_date = os.getenv("EXECUTION_DATE")

spark = SparkSession.builder.appName("transform_events") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# Example schema for Greenery users data
struct_schema = StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("page_url", StringType()),
    StructField("created_at", TimestampType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
])

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

df.createOrReplaceTempView("events")
result = spark.sql("""
    select
        *

    from events
""")

OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/{execution_date}"
result.write.mode("overwrite").parquet(OUTPUT_PATH)