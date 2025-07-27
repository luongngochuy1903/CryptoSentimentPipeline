from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import os
import stat
import time
import json
import trafilatura
from trafilatura.settings import use_config

coins = ["Bitcoin", "Ethereum", "bnb", "xrp", "solana"]

# Cấu hình Chrome
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36")

driver_path = "/usr/local/bin/chromedriver"
if not os.access(driver_path, os.X_OK):
    print("Making chromedriver executable")
    st = os.stat(driver_path)
    os.chmod(driver_path, st.st_mode | stat.S_IEXEC)

driver = webdriver.Chrome(service=Service(driver_path), options=options)
print("✅ Chrome driver khởi tạo thành công")

# Cấu hình trafilatura để dùng User-Agent
config = use_config()
config.set("DEFAULT", "user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36")

for coin in coins:
    url = f'https://www.newsnow.co.uk/h/Business+&+Finance/Cryptocurrencies/{coin}'
    print(f"\n📥 Loading: {url}")
    driver.get(url)
    time.sleep(5)

    # Lấy các thẻ bài viết
    article_links = driver.find_elements(By.CSS_SELECTOR, 'a.article-card__headline')
    print(f"🔎 Tìm thấy {len(article_links)} bài cho {coin}")

    for link_element in article_links:
        intermediate_link = link_element.get_attribute("href")
        title = link_element.text.strip()
        print(f"\n🔗 {title}\n{intermediate_link}")

        try:
            # Truy cập link trung gian để lấy real URL
            driver.get(intermediate_link)
            time.sleep(3)

            # Nhấn nút "Continue to Article >" nếu có
            try:
                continue_button = driver.find_element(By.PARTIAL_LINK_TEXT, "Continue to article")
                continue_button.click()
                time.sleep(3)
            except:
                pass  # Không sao nếu không có nút

            real_url = driver.current_url
            print(f"👉 Real URL: {real_url}")

            # Dùng trafilatura.fetch_url với header cấu hình
            downloaded = trafilatura.fetch_url(real_url, config=config)
            if downloaded:
                content = trafilatura.extract(downloaded)
                if content:
                    item = {
                        "coin": coin,
                        "title": title,
                        "url": real_url,
                        "content": content
                    }
                    print("✅ Trích xuất thành công:")
                    print(json.dumps(item, ensure_ascii=False, indent=2))
                    # with open(f"{coin}_scrape.jsonl", "a", encoding="utf-8") as f:
                    #     f.write(json.dumps(item, ensure_ascii=False) + "\n")
                else:
                    print("⚠️ Không trích xuất được nội dung.")
            else:
                print("❌ Không tải được nội dung bằng fetch_url.")

        except Exception as e:
            print(f"❌ Lỗi khi xử lý bài viết: {e}")

        time.sleep(1)

driver.quit()
print("🎉 Hoàn tất.")
