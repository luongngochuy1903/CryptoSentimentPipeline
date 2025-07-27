import feedparser
import requests
import trafilatura
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse
from playwright.sync_api import sync_playwright

def run_coin():
    rss_feeds = [
        "https://www.coindesk.com/arc/outboundfeeds/rss",            #coindesk
        "https://bitcoinist.com/category/ethereum/feed/",           #etherium
        "https://bitcoinist.com/category/bitcoin/feed/",            #bitcoin
        "https://cointelegraph.com/rss",                            #Cointelegraph
        "https://crypto.news/feed/",                                #CryptoNews
        "https://blockchain.news/rss",                              #Blockchain
        "https://coingeek.com/feed/",                               #coingeek
        "https://cryptodaily.co.uk/feed",                            #cryptoDaily
        "https://coincentral.com/news/feed/",                        #CoinCentral
        "https://www.livebitcoinnews.com/feed/",                     #LiveBitcoin
        "https://crypto-economy.com/feed/"                          #CryptoEconomy
    ]
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    def get_article_urls():
        urls = []
        for feed_url in rss_feeds:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                urls.append({
                    "title": entry.get("title"),
                    "link": entry.get("link"),
                    "published": entry.get("published", None),
                    "author": entry.get("author", None),
                    "source": feed_url
                })
        return urls

    # Bước 2: Dùng trafilatura để lấy nội dung
    def extract_full_text(entry):
        url = entry['link']
        try:
            downloaded = trafilatura.fetch_url(url, request_session=get_session())
            if not downloaded:
                # Nếu trafilatura không hoạt động, dùng playwright để render
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.goto(url, timeout=10000)  # 10s timeout
                    page.wait_for_timeout(3000)    # đợi JS chạy
                    html = page.content()
                    browser.close()

                extracted = trafilatura.extract(html, include_comments=False, include_images=False)
            else:
                extracted = trafilatura.extract(downloaded, include_comments=False, include_images=False)

            return {
                "domain": urlparse(url).netloc,
                "title": entry["title"],
                "url": url,
                "text": extracted,
                "published": entry["published"],
                "author": entry["author"],
                "source": entry["source"],
                "tag": "coin_news"
            }
        except Exception as e:
            return {
                "domain": urlparse(url).netloc,
                "title": entry["title"],
                "url": url,
                "text": None,
                "published": entry["published"],
                "author": entry["author"],
                "source": entry["source"],
                "tag": "coin_news"
            }

    # Bước 3: Threading để tối ưu tốc độ
    def scrape_all_articles():
        urls = get_article_urls()
        results = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(extract_full_text, entry) for entry in urls]
            for future in as_completed(futures):
                results.append(future.result())

        return results

    # Bước 4: Scrape và lưu kết quả
    results = scrape_all_articles()

    threshold_date = datetime(2025, 4, 1, tzinfo=timezone.utc)

    filtered_results = []
    for item in results:
        if item.get("published"):
            try:
                published_date = parse(item["published"])
                if published_date > threshold_date:
                    filtered_results.append(item)
            except Exception as e:
                pass 

    print("OK!" if results else "API does not return any result")
    return filtered_results
    # Ghi ra file
    # with open("rss_coin_filtered_april2025.jsonl", "w", encoding="utf-8") as f:
    #     for item in filtered_results:
    #         json.dump(item, f, ensure_ascii=False)
    #         f.write("\n")
