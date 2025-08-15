from scraping.rss_economy import run_economy
from scraping.rss_politics import run_politics
from dateutil.parser import parse
from scraping.rss_coin import run_coin
from scraping.coin_gnews import run_coin_gnews
from scraping.economy_gnews import run_economy_gnews
from confluent_kafka import Producer
import json, logging, os, sys
from datetime import datetime
log_dir = "/app/scripts/producer/logs"
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join(log_dir, f"news_{timestamp}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

class Batch_News_Manager():
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'kafka:9092'})

    def pushing_kafka(self):
        logger.info("---------------SCRAPING DATA-----------------")
        logger.info("scraping...")
        data_sources = {
            # "economy_news": run_economy(),
            "economy_gnews": run_economy_gnews(),
            # "politics_news": run_politics(),
            # "crypto_news": run_coin(),
            "crypto_gnews": run_coin_gnews(),
            }
        for type, data in data_sources.items():
            logger.info("---------------CHECKING QUALITY-----------------")
            logger.info(f"Data checking for source: {type}")
            self.check_quality(data)
            logger.info("---------------WRITING TO KAFKA-----------------")
            for record in data:
                self.producer.produce(
                    topic="news",
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

    def check_quality(self, results):
        logger.info(f"Sum: {len(results)}")
        dates = [parse(item['published']) for item in results if item['published']]
        logger.info(f"Oldest day: {min(dates).isoformat()}")
        logger.info(f"Nearest day: {max(dates).isoformat()}")

        null_text_count = sum(1 for item in results if item["text"] is None)
        print(f"Text without body text: {null_text_count}")

def main():
    """
    This is the process of news from fetching to pushing Kafka topic:
        1. Run scraping data scripts
        2. Checking data quality
        3. Send to Kafka topic
    """
    manager = Batch_News_Manager()
    manager.pushing_kafka()

if __name__ == "__main__":
    main()

        