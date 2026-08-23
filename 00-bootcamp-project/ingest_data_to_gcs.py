import json

from google.cloud import storage
from google.oauth2 import service_account

DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "gen-lang-client-0322103833"
location = "asia-southeast1"
bucket_name = "deb6-bootcamp-17"
data = "products"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "uploading-file-gcs-buckets.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# Load data from Local to GCS
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

file_path = f"{DATA_FOLDER}/{data}.csv"
dt = "2021-02-10"
destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"

# YOUR CODE HERE TO LOAD DATA TO GCS

service_account_info = json.load(open(keyfile_gcs))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "gen-lang-client-0322103833"

storage_client = storage.Client(
    project=project_id,
    credentials=credentials,
)
bucket = storage_client.bucket(bucket_name)

blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(file_path)

print(
    f"File {file_path} uploaded to {destination_blob_name}."
)