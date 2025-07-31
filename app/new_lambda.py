import os
import boto3
import zipfile
import shutil
import logging

from app.Converter import run_conversion  # Make sure this is in the same directory or packaged correctly

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client
s3_client = boto3.client('s3')

def lambda_handler(event=None, context=None):
    # 1. Get bucket and key from event
    if event is None:
        event = {}
    record = event.get('Records', [{}])[0]
    input_bucket = record.get('s3', {}).get('bucket', {}).get('name', None)
    zip_key = record.get('s3', {}).get('object', {}).get('key', "")

    # Basic validation
    if not zip_key.lower().endswith('.zip') or not input_bucket:
        logger.warning("Skipping non-zip or malformed event.")
        return {
            'statusCode': 400,
            'body': f"Skipped: {zip_key} is not a zip file or event is malformed."
        }
    logger.info(f"Processing {zip_key} from bucket {input_bucket}")

    # Set working paths
    zip_local_path = "/tmp/upload.zip"
    extract_dir = "/tmp/extracted"
    output_dir = "/tmp/processed"
    output_bucket = "jens-output-bucket"

    # Ensure clean workspace
    for d in [extract_dir, output_dir]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)
    #
    #
    # input_dir = "/tmp/extracted"
    # output_dir = "/tmp/processed"
    # os.makedirs(output_dir, exist_ok=True)

    try:
        # 2. Download ZIP file from S3
        s3_client.download_file(input_bucket, zip_key, zip_local_path)
        logger.info(f"Downloaded {zip_key} to {zip_local_path}")

        # 3. Extract ZIP to /tmp/extracted
        with zipfile.ZipFile(zip_local_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        logger.info(f"Extracted ZIP to {extract_dir}")

        # 4. Run conversion logic
        output_files = run_conversion(extract_dir, output_dir)
        logger.info(f"Generated {len(output_files)} output file(s)")

        # 5. Upload output files to output S3 bucket
        for path in output_files:
            filename = os.path.basename(path)
            s3_key = f"converted/{filename}"
            s3_client.upload_file(path, output_bucket, s3_key)
            logger.info(f"Uploaded {s3_key} to {output_bucket}")

        # Cleanup
        shutil.rmtree(extract_dir)
        shutil.rmtree(output_dir)

        return {
            'statusCode': 200,
            'body': f"Successfully processed {zip_key} and uploaded {len(output_files)} file(s)."
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Processing failed: {str(e)}"
        }

