from airflow import DAG
from airflow.operator.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
from s3sensorhook import S3NewFileSensor
import sys, os
sys.path.append("/opt/airflow")
from modules.s3sensorhook  import S3NewFileSensor

with DAG("News with Comments ETL pipelines",
         schedule_interval=None,
         start_date=datetime(2025, 7, 29),
         catchup=False
        )  as dag:
    news_from_source = BashOperator(
        task_id="Run_manager_news",
        bash_command="docker exec kafka-python python scripts/producer/news_source_manager.py"
    )

    news_checking_minio_stage1 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_news",
        aws_access_key="",
        aws_secret_key="",
        endpoint='http://minio:9000',
        bucket="raw",
        prefix="news",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )
    news_upload_to_kafka_stage_2 = BashOperator(
        task_id="Pushing_to_kafka_stage_2_news",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/data_quality/news_quality.py"
    )

    news_consume_from_stage_2 = BashOperator(
        task_id="Consuming_from_kafka_stage_2_news",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/SilverZone/news_to_silver.py"
    )

    news_to_gold = BashOperator(
        task_id="Loading_news_to_gold",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/GoldZone/from_news_to_gold.py"
    )

#-----------------------------COMMENTS-------------------------------
    cmt_from_source = BashOperator(
        task_id="Run_manager_comments",
        bash_command="docker exec kafka-python python scripts/producer/comment_source_manager.py"
    )

    cmt_checking_minio_stage1 = S3NewFileSensor(
        task_id="Checking_raw_file_exists_comments",
        aws_access_key="",
        aws_secret_key="",
        endpoint='http://minio:9000',
        bucket="raw",
        prefix="comments",
        soft_fail=True,
        poke_interval=30,
        timeout=14300,
        mode="poke"
    )

    cmt_upload_to_kafka_stage_2 = BashOperator(
        task_id="Pushing_to_kafka_stage_2_comments",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/data_quality/comment_quality.py"
    )

    cmt_consume_from_stage_2 = BashOperator(
        task_id="Consuming_from_kafka_stage_2_comments",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/SilverZone/comments_to_silver.py"
    )

    cmt_to_gold = BashOperator(
        task_id="Loading_comments_to_gold",
        bash_command="docker exec cryptoreadproject-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark_jobs/consumer/GoldZone/from_comments_to_gold.py"
    )

    dag_finished = EmptyOperator(task_id="stop")

    news_from_source >> news_checking_minio_stage1 >> news_consume_from_stage_2 >> news_upload_to_kafka_stage_2 >> news_to_gold >> dag_finished
    cmt_from_source >> cmt_checking_minio_stage1 >> cmt_consume_from_stage_2 >> cmt_upload_to_kafka_stage_2 >> cmt_to_gold >> dag_finished
    