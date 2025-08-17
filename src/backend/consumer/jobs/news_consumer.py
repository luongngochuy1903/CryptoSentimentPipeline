from consumer import ConsumerManager
from decimal import Decimal
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_values
from minio import Minio, time
from minio.error import S3Error
from io import BytesIO
import os, sys, json, uuid

BASE_URL = os.path.dirname(__file__)
mypath = os.path.abspath(os.path.join(BASE_URL, "../../.."))
sys.path.append(mypath)
from dateutil.parser import parse
from utils.constants import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, POSTGRES_USERNAME, POSTGRES_PASSWORD
from datetime import datetime

class NewsConsumer(ConsumerManager):
    def __init__(self):
        super().__init__()
        self.coin_realtime_consumer = self.create_consumer('news-group-stage1')
        self.postgres_connect = psycopg2.connect(
            host="postgres",
            port="5432",
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            dbname="backend"
        )
        self.cursor = None
        self.minio_client = Minio(
            endpoint='minio:9000',
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

    def handle_msg(self):
        bucket_name = "raw"
        data = self.batch
        now = datetime.utcnow()
        date_partition = datetime.utcnow().strftime("%Y/%m/%d")
        timestamp = datetime.utcnow().strftime("%H%M%S")
        unique_id = uuid.uuid4().hex

        object_name = f"news/{now.strftime('%Y/%m/%d/%H')}/{timestamp}-{unique_id}.json"
        data_bytes = json.dumps(data).encode("utf-8")
        minio_data = BytesIO(data_bytes)
        try:
            self.minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=minio_data,
                length=len(data_bytes),
                content_type="application/json"
            )
            minio_data.close()
            print(f"Successful upload to Minio at {timestamp}")
        except S3Error as e:
            print(f"Failed! {e}")

        #Pushing to postgres
        db_data = []
        try:
            for row in data:
                domain = row.get("domain")
                title = row.get("title")
                url = row.get("url")
                published = parse(row.get("published"))
                author = row.get("author")
                tag = row.get("tag")
                db_data.append((domain, title, url, published, author, tag))
        except Exception as e:
            print(f"Fail: {e}")
        if db_data:
            try:
                self.cursor = self.postgres_connect.cursor()
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO backend.news(domain, title, url, published, author, tag)
                    VALUES %s
                    """,
                    db_data
                )
                self.postgres_connect.commit()
            except Exception as e:
                print(f"Fail to postgres: {e}")
                self.postgres_connect.rollback()
        print("hoàn thành")

if __name__ == "__main__":
    manager = NewsConsumer()
    manager.subcribe_topic(manager.coin_realtime_consumer,"news")
    manager.polling_batch("News-consumer", manager.coin_realtime_consumer)
