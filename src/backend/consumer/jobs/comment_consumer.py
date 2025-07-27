from consumer import ConsumerManager
from decimal import Decimal
from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os, sys, json, uuid, time

BASE_URL = os.path.dirname(__file__)
mypath = os.path.abspath(os.path.join(BASE_URL, "../../.."))
sys.path.append(mypath)
from utils.constants import MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from datetime import datetime

class NewsConsumer(ConsumerManager):
    def __init__(self):
        super().__init__()
        self.coin_realtime_consumer = self.create_consumer('comment-group-stage1')
        self.minio_client = Minio(
            endpoint='minio:9000',
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

    def handle_msg(self):
        bucket_name = "raw"
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)
        data = self.batch
        now = datetime.utcnow()
        date_partition = datetime.utcnow().strftime("%Y/%m/%d")
        timestamp = datetime.utcnow().strftime("%H%M%S")
        unique_id = uuid.uuid4().hex

        object_name = f"comments/{now.strftime('%Y/%m/%d/%H')}/{timestamp}-{unique_id}.json"
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

        print("hoàn thành")

def main():
    manager = NewsConsumer()
    manager.subcribe_topic(manager.coin_realtime_consumer,"comments")
    manager.polling("Comments-consumer", manager.coin_realtime_consumer, 14400)
main()