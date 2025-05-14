from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
import boto3
import os
import zipfile
from io import BytesIO

def download_entire_parsed_data_as_zip(bucket_name, prefix='Round-head6/', zip_file_name='Round-head6.zip', local_download_path='./S3_downloads/'):
    """
    Downloads all files under a given prefix from S3 and zips them locally.
    """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'):  # skip folders
                    continue
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                file_data = file_obj['Body'].read()
                zip_path = key[len(prefix):]
                zipf.writestr(zip_path, file_data)

    # Ensure local directory exists
    os.makedirs(local_download_path, exist_ok=True)

    # Save zip locally
    output_path = os.path.join(local_download_path, zip_file_name)
    with open(output_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    return output_path