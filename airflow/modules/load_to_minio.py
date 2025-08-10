from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io, uuid
from datetime import datetime

def load_postgres_to_minio(**context):
    now = datetime.utcnow()
    timestamp = now.strftime("%H%M%S")
    pg_hook = PostgresHook(postgres_conn_id="postgres_connect")

    query_list = [
        ("SELECT * FROM event WHERE endtime >= '{{prev_execution_date}} AND endtime < {{execution_date}}'", "event"),
        ("SELECT * FROM technical WHERE endtime >= '{{prev_execution_date}} AND endtime < {{execution_date}}", "statistic"),
        ("SELECT * FROM sentiment WHERE endtime >= '{{prev_execution_date}} AND endtime < {{execution_date}}", "sentiment"),
    ]

    for query, key in query_list:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        buffer = io.StringIO()

        cursor.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", buffer)

        s3_hook = S3Hook(aws_conn_id="minio_connect", verify=False)
        unique_id = uuid.uuid4().hex
        s3_hook.load_string(
            string_data=buffer.getvalue(),
            key=f"{key}/{now.strftime('%Y/%m/%d/%H')}/{timestamp}-{unique_id}.json",
            bucket_name="raw",
            replace=False
        )

        cursor.close()
        conn.close()
