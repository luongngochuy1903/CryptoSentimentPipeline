import requests
import json
import trafilatura
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse
from utils.constants import KEY_GNEWS


coins = ["bitcoin", "ethereum", "bnb", "xrp", "solana"]
GNEWS_API_KEY = KEY_GNEWS 
QUERY = ""
LANG = "en"
MAX_ARTICLES = 100

# 1. Gọi GNews API để lấy danh sách bài viết
for coin in coins:
    QUERY = coin
    params = {
        "q": QUERY,
        "lang": LANG,
        "max": MAX_ARTICLES,
        "token": GNEWS_API_KEY
    }
    response = requests.get("https://gnews.io/api/v4/search", params=params)
    data = response.json()
    articles = data.get("articles", [])

    # 2. Hàm scrape nội dung bài viết bằng trafilatura
    def scrape_article(article):
        url = article['url']
        fallback = article.get('description')

        try:
            downloaded = trafilatura.fetch_url(url)
            if not downloaded:
                raise Exception("Failed to download")

            extracted = trafilatura.extract(downloaded, include_comments=False, include_images=False)

            if not extracted:
                raise Exception("Failed to extract")

            return {
                "domain": article.get('source', {}).get('url'),
                "title": article.get('title'),
                "text": extracted,
                "authors": article.get('source', {}).get('name'),
                "published": article.get('publishedAt'),
                "url": url,
                "source": "trafilatura"
            }

        except Exception as e:
            return {
                "domain": article.get('source', {}).get('url'),
                "title": article.get('title'),
                "text": fallback,
                "authors": article.get('source', {}).get('name'),
                "published": article.get('publishedAt'),
                "url": url,
                "source": "gnews_fallback",
                "error": str(e)
            }

    # 3. Dùng threading để tăng tốc scrape
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(scrape_article, article) for article in articles]
        print(f"Tổng số bài: {len(articles)}")
        for future in as_completed(futures):
            results.append(future.result())

    # 4. In ngày cũ nhất và mới nhất
    dates = [parse(item['published']) for item in results if item['published']]
    print(f"Ngày cũ nhất: {min(dates).isoformat()}")
    print(f"Ngày mới nhất: {max(dates).isoformat()}")

    # 5. Ghi file .jsonl
    with open(f"{coin}_news_gnews.jsonl", "w", encoding="utf-8") as f:
        for item in results:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    print(f"Đã hoàn tất ghi dữ liệu vào {coin}_news_gnews.jsonl")
