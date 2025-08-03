from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StringType, LongType, StructField, TimestampType, DecimalType

spark = SparkSession.builder.appName("Streaming_to_app").getOrCreate()

df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subcribe", "real-time") \
        .option("startingOffsets", "latest") \
        .load()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("starttime", LongType(), True),  
    StructField("endtime", LongType(), True),
    StructField("volume", DecimalType(20, 10), True),
    StructField("quotevolume", DecimalType(20, 10), True),
    StructField("open", DecimalType(20, 10), True),
    StructField("close", DecimalType(20, 10), True),
    StructField("highest", DecimalType(20, 10), True),
    StructField("lowest", DecimalType(20, 10), True),
    StructField("tag", StringType(), True)
])

df = df.selectExpr("CAST(value as STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
df = df.withColumn("starttime", from_unixtime(col("starttime") / 1000).cast("timestamp"))
df = df.withColumn("endtime", from_unixtime(col("endtime") / 1000).cast("timestamp"))
df.printSchema()

window_df = df.withWatermark("event")