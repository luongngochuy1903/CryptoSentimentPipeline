from airflow import DAG
from airflow.operator.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
from s3sensorhook import S3NewFileSensor
import sys, os
sys.path.append("/opt/airflow")
from modules.s3sensorhook  import S3NewFileSensor

with DAG("Realtime ETL pipelines",
         schedule_interval=None,
         start_date=datetime(2025, 7, 29),
         catchup=False
        )  as dag:
#------------------------Spark Streaming for kindle price-------------------------
    running_spark_streaming = BashOperator(
        task_id="streaming_to_realtime-app",
        bash_command=""
    )