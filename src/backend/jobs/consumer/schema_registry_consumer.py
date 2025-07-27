from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import Consumer
from pyspark.sql import SparkSession
import os, sys, logging
from datetime import datetime

spark = SparkSession.builder.appName("SchemaRegistryConsumer").config("spark.cores.max", "1").config("spark.executor.cores", "1").getOrCreate()
class SchemaRegistryConsumer():
    def __init__(self, topic, schema, consumer, group_id):
        self.topic = topic
        self.schema = schema
        self.logger = self.create_logging(consumer)
        self.consumer = self.create_consumer(group_id)

    def create_logging(self, consumer):
        log_dir = "/opt/spark_jobs/consumer/logs"
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = os.path.join(log_dir, f"{consumer}_{timestamp}.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        logger = logging.getLogger()
        return logger
    
    def on_partitions_revoked(self, consumer, partitions):
        print("Revoking partitions:", partitions)
        try:
            consumer.commit()
            print("Committed before rebalance")
        except Exception as e:
            self.logger.info("Commit failed before rebalance:", e)

    def on_partitions_assigned(self, consumer, partitions):
        if not partitions:
            self.logger.warning("⚠️ No partitions assigned.")
        print("Assigned partitions:", partitions)

    def dict_to_dict(self, obj, ctx):
        if obj is None:
            return None
        return dict(obj)
    
    def subcribe_topic(self):
        self.consumer.subscribe([self.topic], 
                        on_assign=self.on_partitions_assigned, 
                        on_revoke=self.on_partitions_revoked)
        
    def create_consumer(self, group_id):
        return Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': group_id,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
        })
    
    def handle_msg(self, msg, path):
        df = spark.createDataFrame(msg)
        from pyspark.sql.functions import (
    current_timestamp, year, month, dayofmonth, hour, minute, second
)
        df = df.withColumn("created_ts", current_timestamp()) \
            .withColumn("year", year("created_ts")) \
            .withColumn("month", month("created_ts")) \
            .withColumn("day", dayofmonth("created_ts")) \
            .withColumn("hour", hour("created_ts")) 
        
        df = df.drop("created_ts")
        if "realtime" not in path:
            df.write.mode("append").partitionBy("year", "month", "day") \
                .parquet(f"s3a://silver/{path}/")
        else:
            df.write.mode("append").partitionBy("year", "month", "day", "hour") \
                .parquet(f"s3a://silver/{path}/")

    def polling(self, consumer_name, path):
        json_deserializer = JSONDeserializer(self.schema, from_dict=self.dict_to_dict)
        self.subcribe_topic()
        try:
            batch = []
            while True:
                try:
                    msg = self.consumer.poll(1.0)
                    if msg is None:
                        if batch:
                            try:                          
                                self.handle_msg(batch, path)
                                batch = []
                                self.consumer.commit(asynchronous=True)
                            except Exception as e:
                                self.logger.info(f"handle_msg fail: {e}")
                        continue
                    if msg.error():
                        self.logger.info(f"{consumer_name} error: {msg.error()}")
                        continue
                    python_dict = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    #Need it because json Deserializer is not a part of consumer, so it doesn't know about data source
                    batch.append(python_dict)
                except Exception as e:
                    self.logger.info(f"Fail when consuming messages from {self.topic}: {e}")
                
        except KeyboardInterrupt:
            pass
        finally:
            print(f"Closing consumer {consumer_name}")
            self.consumer.commit()
            self.consumer.close()