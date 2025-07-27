from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import logging

def print_context(**kwargs):
    print("This DAG was triggered by:", kwargs['dag_run'].conf.get("triggered_by"))

@dag(
    dag_id='dw_process',
    start_date=datetime(2025, 5, 12),
    schedule_interval=None,
    catchup=False,
)
def dw_process():
    @task
    def setup_staging():
        command = [
    "docker", "exec",
    "duckdb", "python", "/app/src/datawarehouse_staging.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        logging.info("===== STDOUT =====\n%s", result.stdout)
        logging.info("===== STDOUT =====\n%s", result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        print("completed setting up Staging!")
    
    @task
    def minio_to_duckdb():
        command = [
    "docker", "exec",
    "duckdb", "python", "/app/src/loading_from_minio.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        print("completed loading to Duckdb!")
    
    @task
    def setup_core():
        command = [
    "docker", "exec",
    "duckdb", "python", "/app/src/datawarehouse_core.py"
]

        result = subprocess.run(command, capture_output=True, text=True)
        logging.info("===== STDOUT =====\n%s", result.stdout)
        logging.info("===== STDOUT =====\n%s", result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        print("completed setting up Core!")
    
    @task
    def staging_to_core():
        command = [
    "docker", "exec",
    "duckdb", "python", "/app/src/staging_to_core.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        logging.info("===== STDOUT =====\n%s", result.stdout)
        logging.info("===== STDOUT =====\n%s", result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        print("completed loading to Core! Data warehouse updated...")
    
    setup_staging() >> minio_to_duckdb() >> setup_core() >> staging_to_core()

dw_process()