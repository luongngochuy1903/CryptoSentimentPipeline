import requests
import json
import time
from dateutil.parser import parse

topics = ["bitcoin", "ethereum", "bnb", "xrp", "solana"]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/115.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.reddit.com'
}

results = []

for topic in topics:
    url = f"https://www.reddit.com/r/{topic}/new.json?limit=20"
    print(f"🔍 Scraping r/{topic}...")

    try:
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        data = res.json()

        for post in data['data']['children']:
            p = post['data']
            results.append({
                'subreddit': topic,
                'title': p.get('title'),
                'author': p.get('author'),
                'score': p.get('score'),
                'url': 'https://reddit.com' + p.get('permalink'),
                'created_utc': p.get('created_utc'),
                'id': p.get('id'),
                'selftext': p.get('selftext') or '',
                'num_comments': p.get('num_comments')
            })

    except Exception as e:
        print(f"❌ Lỗi khi scrape r/{topic}: {e}")

    time.sleep(2)  

with open("reddit_crypto_cmt.jsonl", "w", encoding="utf-8") as f:
    for item in results:
        json.dump(item, f, ensure_ascii=False)
        f.write("\n")

print("Hoàn tất scrape và ghi vào reddit_crypto_cmt.jsonl")
