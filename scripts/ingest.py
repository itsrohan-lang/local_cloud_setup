import json
import boto3
import os
from datetime import datetime

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localstack:4566'),
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

# 1. Ensure bucket exists
bucket_name = "datalake"
try:
    s3.create_bucket(Bucket=bucket_name)
except Exception:
    pass

# 2. Generate raw batch data
raw_events = [
    {"order_id": 101, "customer_id": "C1", "amount": 150.50, "currency": "USD", "timestamp": "2024-03-01T10:00:00"},
    {"order_id": 102, "customer_id": "C2", "amount": 25.00,  "currency": "USD", "timestamp": "2024-03-01T10:05:00"},
    {"order_id": 103, "customer_id": "C1", "amount": 75.20,  "currency": "USD", "timestamp": "2024-03-01T10:15:00"},
    {"order_id": 104, "customer_id": "C3", "amount": -10.00, "currency": "USD", "timestamp": "2024-03-01T10:20:00"}, # invalid record for testing validation
]

timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
target_key = f"bronze/transactions/batch_{timestamp_str}.json"

s3.put_object(
    Bucket=bucket_name,
    Key=target_key,
    Body=json.dumps(raw_events).encode('utf-8')
)

print(f"[Ingestion Success] Landed data into s3://{bucket_name}/{target_key}")
