import feedparser
import trafilatura, requests, threading
import json
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse
from playwright.sync_api import sync_playwright

def run_economy():
    print("---------------------RSS ECONOMY----------------------")
    # RSS feeds
    rss_feeds = [
        # The Guardian
        "https://www.theguardian.com/uk/business/rss",
        # CNBC
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        # Economist
        "https://www.economist.com/latest/rss.xml",
        # ft
        "https://www.ft.com/rss/home",
        # globes
        "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=1725",
        # NPR
        "https://www.npr.org/rss/rss.php?id=1006",
        #fortune
        "https://fortune.com/feed/fortune-feeds/?id=3230629",
        #businessStandard
        "https://www.business-standard.com/rss/economy-102.rss",
        "https://www.business-standard.com/rss/finance-103.rss",
        #benziga
        "http://feeds.benzinga.com/benzinga",
        #marketBeat
        "https://www.marketbeat.com/feed/",
        #finance-monthly
        "https://www.finance-monthly.com/feed/",
        #Money
        "https://money.com/money/feed/",
    ]

    # Parse RSS and get URL
    def get_article_urls():
        urls = []
        for feed_url in rss_feeds:
            feed = feedparser.parse(feed_url) #Translate to Python object
            for entry in feed.entries[:10]:
                urls.append({
                    "title": entry.get("title"),
                    "link": entry.get("link"),
                    "published": entry.get("published", None),
                    "author": entry.get("author", None),
                    "source": feed_url
                })
        return urls

    # Trafilatura or Playwright to scraping content
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
                "tag": "economy"
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
                "tag": "economy"
            }

    # Threading
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

    print("OK!" if results else "API does not return any result")
    return results