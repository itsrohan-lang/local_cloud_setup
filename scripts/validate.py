import json
import boto3
import os
import sys

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localstack:4566'),
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

response = s3.list_objects_v2(Bucket="datalake", Prefix="bronze/transactions/")
if 'Contents' not in response:
    print("[Validation Error] No files found in bronze layer!")
    sys.exit(1)

# Pick the latest file
latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]['Key']
obj = s3.get_object(Bucket="datalake", Key=latest_file)
records = json.loads(obj['Body'].read().decode('utf-8'))

print(f"[Validating] File: {latest_file} with {len(records)} records")

# Validation Rules
required_fields = {"order_id", "customer_id", "amount", "currency", "timestamp"}
for r in records:
    # 1. Schema check
    if not required_fields.issubset(r.keys()):
        raise ValueError(f"Schema mismatch on record: {r}")
    # 2. Data constraint check
    if r['amount'] is None:
        raise ValueError(f"Null amount detected on record {r}")

print("[Validation Success] Schema and constraints passed successfully.")
