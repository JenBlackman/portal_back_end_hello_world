import boto3
import os
import zipfile

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    inputbucket = "jens-input-bucket"
    outputbucket = "jens-output-bucket"
    file_to_move = "places.txt"

    # Copy the file from input bucket to output bucket
    copy_source = {'Bucket': inputbucket, 'Key': file_to_move}
    s3_client.copy_object(CopySource=copy_source, Bucket=outputbucket, Key=file_to_move)
    print(f"File {file_to_move} copied from {inputbucket} to {outputbucket}")
    # Optionally, delete the original file from input bucket
    # s3_client.delete_object(Bucket=inputbucket, Key=file_to_move)
    return {
        'statusCode': 200,
        'body': f"File {file_to_move} successfully copied from {inputbucket} to {outputbucket}"
    }

