import json
import os
from google.cloud import storage
from google.oauth2 import service_account

def load_config(config_path="config.json"):
    with open(config_path) as f:
        return json.load(f)

def upload_file_to_gcs(config):
    credentials = service_account.Credentials.from_service_account_file(config["service_account_key_path"])
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(config["bucket_name"])

    destination_blob_name = f"{config['destination_folder']}/{os.path.basename(config['source_file_path'])}"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(config["source_file_path"])

    print(f"✅ Uploaded to gs://{config['bucket_name']}/{destination_blob_name}")

if __name__ == "__main__":
    config = load_config()
    upload_file_to_gcs(config)
