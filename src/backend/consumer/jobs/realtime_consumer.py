from consumer import ConsumerManager
from decimal import Decimal
from confluent_kafka import Consumer, KafkaError
import psycopg2, time
from psycopg2.extras import execute_values
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os, sys, json, uuid

BASE_URL = os.path.dirname(__file__)
mypath = os.path.abspath(os.path.join(BASE_URL, "../../.."))
sys.path.append(mypath)
from dateutil.parser import parse
from utils.constants import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, POSTGRES_USERNAME, POSTGRES_PASSWORD
from datetime import datetime

symbol_name_map = {
    "BTCUSDT": "Bitcoin",
    "ETHUSDT": "Ethereum",
    "BNBUSDT": "BNB",
    "SOLUSDT": "Solana",
    "XRPUSDT": "XRP"
}
class RealTimeConsumer(ConsumerManager):
    def __init__(self):
        super().__init__()
        self.coin_realtime_consumer = self.create_consumer('coin-group-stage1')
        self.postgres_connect = psycopg2.connect(
            host="postgres",
            port="5432",
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            dbname="backend"
        )
        self.cursor = self.postgres_connect.cursor()
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
        timestamp = datetime.utcnow().strftime("%H%M%S")

        object_name = f"realtimecoin/{now.strftime('%Y/%m/%d/%H/%M/%S')}/{timestamp}-{uuid.uuid4().hex}.json"
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
                symbol = row.get("symbol")
                name = symbol_name_map.get(symbol)
                interval = row.get("interval")
                starttime = datetime.fromtimestamp(row.get("starttime")/1000)
                endtime = datetime.fromtimestamp(row.get("endtime")/1000)
                volume = Decimal(row.get("volume"))
                quotevolume = Decimal(row.get("quotevolume"))
                open = Decimal(row.get("open"))
                close = Decimal(row.get("close"))
                highest = Decimal(row.get("highest"))
                lowest = Decimal(row.get("lowest"))
                tag = row.get("tag")
                db_data.append((symbol, name, interval, starttime, endtime, volume, quotevolume, open, close, highest, lowest, tag))
        except Exception as e:
            print(f"Fail: {e}")
        if db_data:
            try:
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO backend.events(symbol, name, interval, starttime, endtime, volume, quotevolume, open, close, highest, lowest, tag)
                    VALUES %s
                    """,
                    db_data
                )
                self.postgres_connect.commit()
            except Exception as e:
                print(f"Fail to postgres: {e}")
                self.postgres_connect.rollback()
        print("hoàn thành")

def main():
    manager = RealTimeConsumer()
    manager.subcribe_topic(manager.coin_realtime_consumer,"real-time")
    manager.polling("realtime_consumer", manager.coin_realtime_consumer, 60)
main()