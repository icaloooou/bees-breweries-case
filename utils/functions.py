# -*- coding: utf-8 -*-

import io
import json
import boto3
import logging
import pyarrow as pa
from utils import config
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

s3_client = boto3.client('s3',
                            region_name=config.region,
                            aws_access_key_id=config.access_key, 
                            aws_secret_access_key=config.secret_key)

def validate_bucket(s3_client, bucket):
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': config.region}
            )
            logger.info(f"Bucket '{bucket}' created.")


def write_data(bucket, data, key, type):    
    validate_bucket(s3_client, bucket)

    if type == 'JSON':
        s3_client.put_object(
            Body=json.dumps(data),
            Bucket=bucket,
            Key=key
        )
    else:
        s3_client.upload_fileobj(data, bucket, key)
    logger.info(f"File ok {key}")


def read_data(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body']


def read_many_data(bucket, key):
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    parquet_files = [obj['Key'] for obj in objects.get('Contents', []) if obj['Key'].endswith('.parquet')]
    tables = []
    for parquet_file in parquet_files:
        response = s3_client.get_object(Bucket=bucket, Key=parquet_file)
        file_content = response['Body'].read()
        buffer = io.BytesIO(file_content)
        table = pq.read_table(buffer)
        tables.append(table)
    combined_table = pa.concat_tables(tables)
    df = combined_table.to_pandas()
    return df
