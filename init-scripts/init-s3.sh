#!/bin/bash
echo "==> [Auto-Init] Initializing S3 Buckets..."

# Create default buckets
awslocal s3 mb s3://datalake
awslocal s3 mb s3://test1

# If there is backup data on the host, sync it into S3
if [ -d "/s3-backup/datalake" ]; then
    echo "==> [Auto-Init] Restoring datalake from local disk..."
    awslocal s3 sync /s3-backup/datalake s3://datalake
fi

if [ -d "/s3-backup/test1" ]; then
    echo "==> [Auto-Init] Restoring test1 from local disk..."
    awslocal s3 sync /s3-backup/test1 s3://test1
fi

echo "==> [Auto-Init] S3 buckets ready!"
