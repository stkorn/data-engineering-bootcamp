from pyspark.sql import SparkSession  # pyright: ignore[reportMissingImports]
from pyspark.sql.types import (  # pyright: ignore[reportMissingImports]
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)

BUSINESS_DOMAIN = "greenery"
BUCKET_NAME = "deb6-bootcamp-17"
DATA = "products"
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

spark = SparkSession.builder.appName("transform_product") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

struct_schema = StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("price", FloatType()),
    StructField("inventory", IntegerType()),
])

GCS_FILE_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"

# df = spark.read \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .csv(GCS_FILE_PATH)

df = spark.read \
    .option("header", True) \
    .schema(struct_schema) \
    .csv(GCS_FILE_PATH)

df.show()

df.createOrReplaceTempView("products")
result = spark.sql("""
    select
        *

    from products
""")

OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/"
result.write.mode("overwrite").parquet(OUTPUT_PATH)