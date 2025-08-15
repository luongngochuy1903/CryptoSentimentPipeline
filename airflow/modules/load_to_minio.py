from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import uuid, json
from datetime import datetime
import pandas as pd

def load_postgres_to_minio(**context):
    now = datetime.utcnow()
    timestamp = now.strftime("%H%M%S")
    pg_hook = PostgresHook(postgres_conn_id="postgres_connect")
    s3_hook = S3Hook(aws_conn_id="minio_connect", verify=False)

    query_list = [
        ("SELECT * FROM event WHERE endtime >= NOW() - INTERVAL '1 hour' AND endtime < NOW()", "realtimecoin"),
        ("SELECT * FROM technical WHERE endtime >= NOW() - INTERVAL '1 hour' AND endtime < NOW()", "statistic"),
        ("SELECT * FROM sentiment WHERE endtime >= NOW() - INTERVAL '1 hour' AND endtime < NOW()", "sentiment"),
    ]


    conn = pg_hook.get_conn()

    for query, key in query_list:
        df = pd.read_sql(query, conn)
        print(f"độ dài df: {len(df)}")
        print(f"Các cột: {df.columns.tolist()}")
        print(f"Kiểu dữ liệu:\n{df.dtypes}")
        json_data = df.to_json(orient="records", date_format="iso")

        unique_id = uuid.uuid4().hex
        s3_hook.load_string(
            string_data=json_data,
            key=f"{key}/{now.strftime('%Y/%m/%d/%H')}/{timestamp}-{unique_id}.json",
            bucket_name="raw",
            replace=False
        )

    conn.close()
