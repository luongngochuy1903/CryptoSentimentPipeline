from consumer import ConsumerManager
from decimal import Decimal
import pandas as pd
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
        self.cursor = None
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
        print(data)
        #Pushing to postgres
        db_data = []
        try:
            for row in data:
                symbol = row.get("symbol")
                name = symbol_name_map.get(symbol)
                interval = row.get("interval")
                starttime = datetime.fromtimestamp(row.get("starttime")/1000).replace(microsecond=0)
                endtime = datetime.fromtimestamp(row.get("endtime")/1000).replace(microsecond=0)
                volume = Decimal(row.get("volume"))
                quotevolume = Decimal(row.get("quotevolume"))
                open = Decimal(row.get("open"))
                close = Decimal(row.get("close"))
                highest = Decimal(row.get("highest"))
                lowest = Decimal(row.get("lowest"))
                tag = row.get("tag")
                db_data.append((symbol, name, interval, starttime, endtime, volume, quotevolume, open, close, highest, lowest, tag))
                print(f"hehe: {endtime}")
        except Exception as e:
            print(f"Fail: {e}")
        if db_data:
            try:
                self.cursor = self.postgres_connect.cursor()
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO event(symbol, name, interval, starttime, endtime, volume, quotevolume, open, close, highest, lowest, tag)
                    VALUES %s
                    """,
                    db_data
                )
                self.postgres_connect.commit() #Use for INSERT/UPDATE to commit change
            except Exception as e:
                print(f"Fail to postgres: {e}")
                self.postgres_connect.rollback()
            finally:
                self.cursor.close()
        print("hoàn thành")

if __name__ == "__main__":
    manager = RealTimeConsumer()
    manager.subcribe_topic(manager.coin_realtime_consumer,"real-time")
    manager.polling("realtime_consumer", manager.coin_realtime_consumer)
