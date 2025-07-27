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
        coindf = spark.read.json(f"s3a://raw/realtimecoin/{latest.year}/{latest.month:02}/{latest.day:02}/{latest.hour:02}/{latest.minute:02}/{latest.second:02}")
        self.logger.info(coindf.schema.simpleString())
        coindf.printSchema()

        #Checking null
        from pyspark.sql.functions import col
        num_of_null_text = coindf.filter((col("text").isNull()) | (col("text") == "")).count()
        self.logger.info(f"Text columns WHICH IS NULL:{num_of_null_text}")

        num_of_null_published = coindf.filter(col("published").isNull()).count()
        self.logger.info(f"Published columns WHICH IS NULL:{num_of_null_published}")

        #Cheking date
        from pyspark.sql.functions import col, from_unixtime, cast
        coindf = coindf.withColumn("starttime", from_unixtime(col("starttime").cast("int")/1000))
        coindf = coindf.withColumn("endtime", from_unixtime(col("endtime").cast("int")/1000))

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
            "required": ["symbol", "name", "interval", "starttime", "endtime", "volume", "quotevolume", "open", "close", "highest", "lowest", "tag"]
        }
        """

    obj = RealtimeQualityProducer(schema, "realtime_raw", "Realtime")
    obj.setCompatibility("BACKWARD")
    df = obj.data_quality()
    obj.produces(df)
    print("Xong")

if __name__ == "__main__":
    main()