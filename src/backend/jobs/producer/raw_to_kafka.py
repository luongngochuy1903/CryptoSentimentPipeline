from pyspark.sql import SparkSession
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import requests
import json, logging, os, sys
from datetime import datetime

spark = SparkSession.builder.appName("sendTokafka").getOrCreate()

class SchemaRegistryObj():
    def __init__(self, schema, topic, producer):
        self.schema = schema
        self.client = SchemaRegistryClient({"url":"http://schema-registry:8081"})
        self.producer = Producer({"bootstrap.servers":"kafka:9092"})
        self.topic = topic
        self.logger = self.create_logging(producer)

    def create_logging(self, producer):
        log_dir = "/opt/spark_jobs/producer/logs"
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = os.path.join(log_dir, f"{producer}_{timestamp}.log")
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

    def row_to_dict(self, row, ctx):
        return dict(row)
    
    def setCompatibility(self, level):
        input = {"compatibility":level}
        schema_registry_url = "http://schema-registry:8081"

        response = requests.put(
            f"{schema_registry_url}/config",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json=input
        )
        if response.status_code == 200:
            self.logger.info(f"Schema registry is set to {level}")
        else:
            self.logger.info("Fail to set compatibility level")
    def acked(self, err, msg):
        if err:
            self.logger.error("Failed to deliver message")
        else:
           self.logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    def produces(self, data):
        json_serializer = JSONSerializer(self.schema, self.client, self.row_to_dict)
        key_serializer = StringSerializer()

        for row in data.collect():
            row_dict = row.asDict()
            value = json_serializer(row_dict, SerializationContext(self.topic, MessageField.VALUE))
            # key = key_serializer("id1", SerializationContext(self.topic, MessageField.KEY))
            self.producer.produce(topic=self.topic, value=value, callback=self.acked)
        self.producer.flush()



