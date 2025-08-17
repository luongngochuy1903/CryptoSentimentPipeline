import feedparser
import trafilatura, requests, threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse
from playwright.sync_api import sync_playwright

def run_politics():
    print("---------------------RSS POLITICS----------------------")
    rss_feeds = [
        "https://www.theguardian.com/politics/rss",                      # The Guardian Politics
        "https://www.theguardian.com/world/war/rss",
        "https://www.cbsnews.com/latest/rss/politics",                  #CBS
        "https://rss.politico.com/defense.xml",                         # Politico
        "https://thehill.com/rss/syndicator/19110",                     # The Hill
        "https://www.foreignaffairs.com/rss.xml",                       # Foreign Affairs 
        "https://www.globalissues.org/news/feed",                       # Global Issues
        "https://www.thenation.com/subject/politics/feed/",             # The nation
        "https://www.aljazeera.com/xml/rss/all.xml",                     #aljazeera
        "https://feeds.npr.org/1014/rss.xml",                            # NPR
        "https://moxie.foxnews.com/google-publisher/politics.xml",       # Fox
        "https://www.baltimoresun.com/news/politics/feed/",                #Baltimore
        "https://www.politicshome.com/news/rss"
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

    # Bước 2: Trafilatura or Playwright to scraping content
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
                "tag": "politics"
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
                "tag": "politics"
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

    print("OK!" if results else "API does not return any result")
    return results