from news_to_silver import run_news_consumer
from comments_to_silver import run_comments_consumer
from realtime_to_silver import run_realtime_consumer
from multiprocessing import Process

if __name__ == "__main__":
    processes = [
        Process(target=run_news_consumer),
        Process(target=run_comments_consumer),
        Process(target=run_realtime_consumer),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
