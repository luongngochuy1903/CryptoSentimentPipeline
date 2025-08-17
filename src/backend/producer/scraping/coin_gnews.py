import requests
import json
import trafilatura, requests, threading
import os, sys
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
BASE_URL = os.path.dirname(__file__)
mypath = os.path.abspath(os.path.join(BASE_URL, "../../.."))
sys.path.append(mypath)
from dateutil.parser import parse
from utils.constants import KEY_GNEWS

def run_coin_gnews():
    print("---------------------COIN GNEWS----------------------")
    coins = ["bitcoin", "ethereum", "bnb", "xrp", "solana"]
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    results = []
    for coin in coins:
        response = requests.get(f"https://gnews.io/api/v4/search?q={coin}&lang=en&apikey={KEY_GNEWS}")
        data = response.json()
        articles = data.get("articles", [])

        def scrape_article(article):
            url = article['url']
            fallback = article.get('description')

            try:
                downloaded = trafilatura.fetch_url(url, request_session=get_session())
                if not downloaded:
                    raise Exception("Failed to download")

                extracted = trafilatura.extract(downloaded, include_comments=False, include_images=False)

                if not extracted:
                    raise Exception("Failed to extract")

                return {
                    "domain": article.get('source', {}).get('url'),
                    "title": article.get('title'),
                    "url": url,
                    "text": extracted,                
                    "published": article.get('publishedAt'),      
                    "authors": article.get('source', {}).get('name'),            
                    "source": "trafilatura",
                    "tag": "coins_news"
                }

            except Exception as e:
                return {
                    "domain": article.get('source', {}).get('url'),
                    "title": article.get('title'),
                    "url": url,
                    "text": fallback,                   
                    "published": article.get('publishedAt'),
                    "authors": article.get('source', {}).get('name'),
                    "source": "gnews_fallback",
                    "tag": "coins_news"
                }

        # 3. Threading
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(scrape_article, article) for article in articles]
            print(f"Sum of news: {len(articles)}")
            for future in as_completed(futures):
                results.append(future.result())

    return results