from google.cloud import storage
from google.oauth2 import service_account

import os

def bucket(file, bucket, upload = True):
    credentials = service_account.Credentials.from_service_account_file(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

    client = storage.Client(
        project='bq-central',
        credentials=credentials
    )

    bucket = client.get_bucket(bucket)

    if upload == True:
        blob = bucket.blob(file)
        blob.upload_from_filename(file)