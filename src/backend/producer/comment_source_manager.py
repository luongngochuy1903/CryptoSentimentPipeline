from scraping.reddit import run_comment
from confluent_kafka import Producer
from dateutil.parser import parse
import json, logging, os, sys
from datetime import datetime
log_dir = "/app/scripts/producer/logs"
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join(log_dir, f"comment_{timestamp}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

class Batch_Comment_Manager():
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'kafka:9092'})

    def pushing_kafka(self):
        logger.info("---------------SCRAPING DATA-----------------")
        logger.info("scraping...")
        data_sources = {
            "reddit_comment": run_comment(),
            }
        for type, data in data_sources.items():
            logger.info("---------------CHECKING QUALITY-----------------")
            logger.info(f"Data checking for source: {type}")
            self.check_quality(data)
            logger.info("---------------WRITING TO KAFKA-----------------")
            for record in data:
                self.producer.produce(
                    topic="comments",
                    value=json.dumps(record),
                    callback=self.acked
                )
        self.producer.flush()
        logger.info("TASK COMPLETED !")
    def acked(self, err, msg):
        if err:
            logger.error("Failed to deliver message")
        else:
            logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    def check_quality(results):
        logger.info(f"Tổng số bài: {len(results)}")
        dates = [parse(item['created_utc']) for item in results if item['created_utc']]
        logger.info(f"Ngày cũ nhất: {min(dates).isoformat()}")
        logger.info(f"Ngày mới nhất: {max(dates).isoformat()}")

        null_text_count = sum(1 for item in results if item["selftext"] is None)
        print(f"Số bài viết không lấy được nội dung: {null_text_count}")

def main():
    """
    This is the process of comments from fetching to pushing Kafka topic:
        1. Run scraping data scripts
        2. Checking data quality
        3. Send to Kafka topic
    """
    manager = Batch_Comment_Manager()
    manager.pushing_kafka()

if __name__ == "__main__":
    main()

        