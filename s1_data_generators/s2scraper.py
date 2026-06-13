import csv
import hashlib
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

from s0config_schema_kafka import BASE_URL

def get_data(driver, url: str):
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "title"))
            )
        except Exception:
            time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        title = soup.find("title")
        pub = soup.find("span", {"data-role": "publishdate"})
        cate = soup.find("a", {"data-role": "cate-name"})
        snippet = soup.find("p", {"data-role": "sapo"})
        tag_div = soup.find("div", {"data-marked-zoneid": "cafef_detail_tag"})

        return {
            "url": url,
            "article_id": hashlib.sha256(url.encode()).hexdigest(),
            "title": title.text.strip() if title else None,
            "pub_date": pub.get_text(strip=True) if pub else None,
            "cate_source": cate.get_text(strip=True) if cate else None,
            "snippet": snippet.get_text(strip=True) if snippet else None,
            "key_words": [
                a.get_text(strip=True)
                for a in tag_div.find_all("a")
            ] if tag_div else [],
            "related_news": [
                {
                    "title": li.a["title"].strip(),
                    "link": urljoin(BASE_URL, li.a["href"])
                }
                for li in soup.select("ul.tinlienquan li")
                if li.a and li.a.get("href") and li.a.get("title")
            ],
            "crawled_at": datetime.utcnow().isoformat()
        }

    except Exception:
        return None


def load_url(csv_file: str):
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"].strip().lower() != "processed":
                yield row


def mark_processed(csv_file: str, processed_hashes: set):
    tmp_file = csv_file + ".tmp"
    with open(csv_file, newline="", encoding="utf-8") as fin, \
         open(tmp_file, "w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if row["url_hash"] in processed_hashes:
                row["status"] = "processed"
            writer.writerow(row)

    Path(tmp_file).replace(csv_file)