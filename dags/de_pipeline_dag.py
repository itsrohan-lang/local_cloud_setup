from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='end_to_end_de_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Run Data Ingestion on EC2 Compute Node
    step_ingest = BashOperator(
        task_id='1_ingest_data_to_s3',
        bash_command='docker exec de_ec2_node python /app/scripts/ingest.py'
    )

    # 2. Run Data Validation
    step_validate = BashOperator(
        task_id='2_validate_bronze_data',
        bash_command='docker exec de_ec2_node python /app/scripts/validate.py'
    )

    # 3. Submit ETL & Modeling job to EMR (Spark)
    step_emr_spark_etl = BashOperator(
        task_id='3_run_emr_spark_etl',
        bash_command="""
        docker exec de_emr_master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /opt/spark/scripts/etl_modeling.py
        """
    )
    # Define execution graph
    step_ingest >> step_validate >> step_emr_spark_etl
