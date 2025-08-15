from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

from datetime import datetime
import sys, os
sys.path.append("/opt/airflow")
from utils.constants import MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from modules.s3sensorhook  import S3NewFileSensor
from modules.load_to_minio  import load_postgres_to_minio

with DAG("Realtime_ETL_pipelines",
         schedule_interval=None,
         start_date=datetime(2025, 7, 29),
         catchup=False
        )  as dag:
#------------------------Spark Streaming for kindle price-------------------------

    loading_to_raw = PythonOperator(
        task_id="Load_from_postgres_to_minio",
        python_callable=load_postgres_to_minio
    )

    realtime_checking_minio_stage1 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_realtime",
        aws_access_key=MINIO_ACCESS_KEY,
        aws_secret_key=MINIO_SECRET_KEY,
        endpoint='http://minio:9000',
        bucket="raw",
        prefix="realtime",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    realtime_upload_to_kafka_stage_2 = BashOperator(
        task_id="Pushing_to_kafka_stage_2_realtime",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/data_quality/realtime_quality.py"
    )

    realtime_upload_to_silverZone_stage_2 = BashOperator(
        task_id="Pushing_to_silverZone_stage_2_realtime",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/SilverZone/realtime_to_silver.py"
    )

    realtime_checking_minio_stage2 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_realtime_stage2",
        aws_access_key=MINIO_ACCESS_KEY,
        aws_secret_key=MINIO_SECRET_KEY,
        endpoint='http://minio:9000',
        bucket="silver",
        prefix="realtime",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    realtime_to_gold = BashOperator(
        task_id="Loading_realtime_to_gold",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/GoldZone/from_realtime_to_gold.py"
    )

    #-------------------------------statistic and sentiment--------------------------------------------
    sta_checking_minio_stage1 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_statistic",
        aws_access_key=MINIO_ACCESS_KEY,
        aws_secret_key=MINIO_SECRET_KEY,
        endpoint='http://minio:9000',
        bucket="raw",
        prefix="statistic",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    sen_checking_minio_stage1 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_sentiment",
        aws_access_key=MINIO_ACCESS_KEY,
        aws_secret_key=MINIO_SECRET_KEY,
        endpoint='http://minio:9000',
        bucket="raw",
        prefix="sentiment",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    sta_upload_to_kafka_stage_2 = BashOperator(
        task_id="Pushing_to_kafka_stage_2_backend_data",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/data_quality/statistic_sen_quality.py"
    )

    sta_sen_upload_to_silverZone_stage_2 = BashOperator(
        task_id="Pushing_to_silverZone_stage_2_backend_data",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/SilverZone/sta_sen_to_silver.py"
    )

    sta_checking_minio_stage2 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_statistic_stage2",
        aws_access_key=MINIO_ACCESS_KEY,
        aws_secret_key=MINIO_SECRET_KEY,
        endpoint='http://minio:9000',
        bucket="silver",
        prefix="statistic",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    sen_checking_minio_stage2 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_sentiment_stage2",
        aws_access_key=MINIO_ACCESS_KEY,
        aws_secret_key=MINIO_SECRET_KEY,
        endpoint='http://minio:9000',
        bucket="silver",
        prefix="sentiment",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    sta_to_gold = BashOperator(
        task_id="Loading_backend_to_gold",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/GoldZone/from_statistic_sentiment_to_gold.py"
    )
    
    chain(
        realtime_checking_minio_stage1,
        realtime_upload_to_kafka_stage_2,
        realtime_upload_to_silverZone_stage_2,
        realtime_checking_minio_stage2,
        realtime_to_gold
    )

    chain(
        [sta_checking_minio_stage1, sen_checking_minio_stage1],
        sta_upload_to_kafka_stage_2,
        sta_sen_upload_to_silverZone_stage_2,
        [sta_checking_minio_stage2, sen_checking_minio_stage2],
        sta_to_gold
    )

    loading_to_raw >> [realtime_checking_minio_stage1, sta_checking_minio_stage1, sen_checking_minio_stage1]