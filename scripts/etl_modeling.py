from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count

# Setup Spark with LocalStack S3 S3A connector
spark = SparkSession.builder \
    .appName("EMR-Data-Modeling-ETL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Reduce verbose Spark logging
spark.sparkContext.setLogLevel("WARN")

print("--> Reading Bronze data from LocalStack S3...")
raw_df = spark.read.json("s3a://datalake/bronze/transactions/*.json")

# 1. Clean & Filter
clean_df = raw_df.filter(col("amount") > 0)

# 2. Data Modeling: Aggregate into Customer Summary Fact Table
dim_customer_summary = clean_df.groupBy("customer_id").agg(
    _sum("amount").alias("total_spent"),
    _count("order_id").alias("total_orders")
)

print("--> Transformed Model Result:")
dim_customer_summary.show()

# 3. Write Gold Parquet Model
output_path = "s3a://datalake/gold/dim_customer_summary"
dim_customer_summary.write.mode("overwrite").parquet(output_path)

print(f"--> [ETL Success] Dimensional model saved to {output_path}")
spark.stop()
