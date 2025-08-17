import feedparser
import requests
import trafilatura
import threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse
from playwright.sync_api import sync_playwright

def run_coin():
    print("---------------------RSS COIN----------------------")
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

    def get_article_urls():
        urls = []
        for feed_url in rss_feeds:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:10]:
                urls.append({
                    "title": entry.get("title"),
                    "link": entry.get("link"),
                    "published": entry.get("published", None),
                    "author": entry.get("author", None),
                    "source": feed_url
                })
        return urls

    # Bước 2: trafilatura scraping context
    def extract_full_text(entry):
        url = entry['link']
        try:
            downloaded = trafilatura.fetch_url(url)
            if not downloaded:
                # Using playwright if trafilatura doesn't work
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.goto(url, timeout=10000)  # 10s timeout
                    page.wait_for_timeout(3000)    # wait for JS running completely
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

    # Bước 3: Threading
    def scrape_all_articles():
        urls = get_article_urls()
        results = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(extract_full_text, entry) for entry in urls]
            print(f"Sum of news: {len(urls)}")
            for future in as_completed(futures):
                results.append(future.result())

        return results

    results = scrape_all_articles()
    return results
