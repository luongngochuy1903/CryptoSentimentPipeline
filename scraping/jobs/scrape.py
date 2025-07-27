from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import trafilatura

coins = ["Bitcoin", "Ethereum", "bnb", "xrp", "solana"]

options = Options()
options.add_argument('--headless') 
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

for coin in coins:
    url = f'https://www.newsnow.co.uk/h/Business+&+Finance/Cryptocurrencies/{coin}'
    print(f"\n📥 Loading: {url}")
    driver.get(url)
    time.sleep(5) 

    article_links = driver.find_elements(By.CSS_SELECTOR, 'a.hl')
    print(f"🔎 Tìm thấy {len(article_links)} bài cho {coin}")

    for link_element in article_links:
        link = link_element.get_attribute("href")
        title = link_element.text.strip()

        print(f"\n🔗 {title}\n{link}")

        downloaded = trafilatura.fetch_url(link)
        if downloaded:
            content = trafilatura.extract(downloaded)
            if content:
                item = {
                    "coin": coin,
                    "title": title,
                    "url": link,
                    "content": content
                }
                with open(f"{coin}_scrape.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
            else:
                print("Không trích xuất được nội dung.")
        else:
            print("Không tải được URL.")

        time.sleep(1)

driver.quit()
