import boto3
import os
import zipfile
from io import BytesIO

def download_entire_parsed_data_as_zip(bucket_name, prefix='Round-head6/', zip_file_name='Round-head6.zip', local_download_path='./S3_downloads/'):
    """
    Downloads all files under 'parsed_data/' from the given S3 bucket and zips them into a single file.
    
    Parameters:
    - bucket_name: str, name of the S3 bucket
    - prefix: str, prefix in S3 (default: 'parsed_data/')
    - zip_file_name: str, name of the zip file to save
    - local_download_path: str, local directory to save the file
    """
    
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'):  # skip "folders"
                    continue
                # print(f"Downloading: {key}")
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                file_data = file_obj['Body'].read()

                # Strip the prefix to keep relative folder structure in ZIP
                zip_path = key[len(prefix):]
                zipf.writestr(zip_path, file_data)

    # Save zip locally
    output_path = os.path.join(local_download_path, zip_file_name)
    with open(output_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    print(f"\n✅ ZIP saved to: {output_path}")

# Example usage
if __name__ == "__main__":
    download_entire_parsed_data_as_zip(bucket_name='parsed-structured-data')
