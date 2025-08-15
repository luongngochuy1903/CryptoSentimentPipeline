from confluent_kafka import Consumer, KafkaError
import time, json
from datetime import datetime

class ConsumerManager():
    def on_partitions_revoked(self, consumer, partitions):
        print("Revoking partitions:", partitions)
        try:
            consumer.commit()
            print("Committed before rebalance")
        except Exception as e:
            print("Commit failed before rebalance:", e)

    def on_partitions_assigned(self, consumer, partitions):
        print("Assigned partitions:", partitions)

    def __init__(self):
        self.batch = []

    def create_consumer(self, group_id):
        return Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': group_id,
            'enable.auto.commit': False,
            "heartbeat.interval.ms": 3000,
            'auto.offset.reset': 'latest'
        })
    
    def subcribe_topic(self, consumer, topic):
        consumer.subscribe([topic], 
                        on_assign=self.on_partitions_assigned, 
                        on_revoke=self.on_partitions_revoked)

    def handle_msg(self):
        pass
    def polling(self, consumer_name, consumer):
        print(f"Start consuming: {consumer_name}")
        try:
            while True:
                msg = consumer.consume(timeout=1.0)
                if not msg:
                    continue
                if len(self.batch) >= 5:
                    print(f"{consumer_name} received: {self.batch}")
                    try:
                        self.handle_msg() 
                        self.batch = [] 
                        consumer.commit(asynchronous=True)
                    except Exception as e:
                        print(f"handle_msg fail: {e}")
                
                for message in msg:
                    try:
                        if message.error():
                            print(f"{consumer_name} error: {message.error()}")
                            continue
                        print(f"Offset: {message.offset()}")
                        value = json.loads(message.value().decode("utf-8"))
                        self.batch.append(value)
                        print(f"Batch size: {len(self.batch)}")
                    except Exception as e:
                        print("Failed to parse message while batching:", e)

        except KeyboardInterrupt:
            pass
        finally:
            print(f"Closing consumer: {consumer_name}")
            consumer.commit()
            consumer.close()
    
    def polling_batch(self, consumer_name, consumer):
        print(f"Start consuming: {consumer_name}")
        start_time = time.time()
        try:
            while True:
                msg = consumer.consume(timeout=1.0)
                if msg:
                    for message in msg:
                        if message.error():
                            print(f"{consumer_name} error: {message.error()}")
                            continue
                        value = json.loads(message.value().decode("utf-8"))
                        self.batch.append(value)

                # Điều kiện dừng
                if (time.time() - start_time) > 60:
                    print(f"{consumer_name} finished batch with {len(self.batch)} messages")
                    break
        finally:
            consumer.commit()
            consumer.close()