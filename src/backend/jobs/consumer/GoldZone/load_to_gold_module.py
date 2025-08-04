from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
now = datetime.now()
spark = SparkSession.builder.appName("Loading to Gold zone").getOrCreate()

def get_latest(table):
    latest_partition = spark.sql(f"""
        SELECT year, month, day
        FROM {table}
        ORDER BY year DESC, month DESC, day DESC, hour DESC
        LIMIT 1
    """).collect()[0]
    return latest_partition
    