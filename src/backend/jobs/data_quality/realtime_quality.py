import logging, os, sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from module.modules import get_latest_partition_datetime
from producer.raw_to_kafka import SchemaRegistryObj

class RealtimeQualityProducer(SchemaRegistryObj):
    def data_quality(self):
        self.logger.info("------------------REALTIME COIN DATA QUALITY------------------")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Data quality check") \
            .getOrCreate()
        latest = get_latest_partition_datetime("raw", "realtimecoin")
        coindf = spark.read.json(f"s3a://raw/realtimecoin/{latest.year}/{latest.month:02}/{latest.day:02}/{latest.hour:02}")
        self.logger.info(coindf.schema.simpleString())
        coindf.printSchema()
        
        #Cheking date
        from pyspark.sql.functions import col, to_timestamp, cast
        coindf = coindf.withColumn("starttime", to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss"))
        coindf = coindf.withColumn("endtime", to_timestamp(col("endtime"), "yyyy-MM-dd HH:mm:ss"))

        #Checking volume, price
        coindf = coindf.withColumn("volume", col("volume").cast('double'))
        coindf = coindf.withColumn("quotevolume", col("quotevolume").cast('double'))
        coindf = coindf.withColumn("open", col("open").cast('double'))
        coindf = coindf.withColumn("close", col("close").cast('double'))
        coindf = coindf.withColumn("highest", col("highest").cast('double'))
        coindf = coindf.withColumn("lowest", col("lowest").cast('double'))

        #Checking datatypes
        for column, dtype in coindf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")

        return coindf

def main():
    schema = """
        {
            "$schema":"http://json-schema.org/draft-07/schema#",
            "title":"News",
            "type":"object",
            "properties": {
                "realtime_id":{"type":"number"},
                "symbol": {"type":"string"},
                "name": {"type":"string"},
                "interval": {"type":"string"},
                "starttime": {"type":"string", "format":"date-time"},
                "endtime": {"type":"string", "format":"date-time"},
                "volume": {"type":"number"},
                "quotevolume": {"type":"number"},
                "open": {"type":"number"},
                "close": {"type":"number"},
                "highest": {"type":"number"},
                "lowest": {"type":"number"},
                "tag": {"type":"string"}
            },
            "required": ["realtime_id", "symbol", "name", "interval", "starttime", "endtime", "volume", "quotevolume", "open", "close", "highest", "lowest", "tag"]
        }
        """

    obj = RealtimeQualityProducer(schema, "realtime_raw", "Realtime")
    obj.setCompatibility("BACKWARD")
    df = obj.data_quality()
    obj.produces(df)
    print("Xong")

if __name__ == "__main__":
    main()