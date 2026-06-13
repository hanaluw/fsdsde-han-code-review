import csv
import json
import hashlib
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from bs4 import BeautifulSoup
import time
from datetime import datetime
from pathlib import Path


# ================= CONFIG =================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "news_topic"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

CSV_FILE = Path(__file__).parent / "output/cafef_articles/articles.csv"
BASE_URL = "https://cafef.vn"
DLQ_TOPIC = "news_dlq"

MAX_RETRY = 3


# ================= SCHEMA =================
NEWS_SCHEMA_STR = """
{
  "type": "record",
  "name": "NewsArticle",
  "fields": [
    {"name": "article_id", "type": "string"},
    {"name": "url", "type": "string"},
    {"name": "title", "type": ["null", "string"], "default": null},
    {"name": "pub_date", "type": ["null", "string"], "default": null},
    {"name": "cate_source", "type": ["null", "string"], "default": null},
    {"name": "snippet", "type": ["null", "string"], "default": null},

    {"name": "key_words", "type": {"type": "array", "items": "string"}, "default": []},

    {
      "name": "related_news",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "RelatedNews",
          "fields": [
            {"name": "title", "type": "string"},
            {"name": "link", "type": "string"}
          ]
        }
      },
      "default": []
    },

    {"name": "crawled_at", "type": "string"}
  ]
}
"""


# ================= KAFKA =================
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

avro_serializer = AvroSerializer(
    schema_registry_client,
    NEWS_SCHEMA_STR
)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": 50,
    "batch.num.messages": 1000,
    "acks": "all",
    "retries": 5,
    "enable.idempotence": True
})


def delivery_report(err, msg):
    if err:
        print(f"[ERR] delivery failed: {err}")


def send_dlq(record):
    producer.produce(DLQ_TOPIC, json.dumps(record, ensure_ascii=False).encode("utf-8"))


def send_with_retry(record, attempt=0):
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            value=avro_serializer(
                record,
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            ),
            on_delivery=delivery_report
        )
    except Exception:
        if attempt < MAX_RETRY:
            send_with_retry(record, attempt + 1)
        else:
            send_dlq(record)


# ================= DRIVER =================
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# ================= SCRAPER =================
def get_data(driver, url: str):
    try:
        driver.get(url)

        # FIX #5: Dùng WebDriverWait thay vì sleep cố định
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "title"))
            )
        except Exception:
            # Fallback nếu wait timeout
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


# ================= CSV =================
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


# ================= PIPELINE =================
def crawl_to_kafka(items):
    driver = create_driver()
    processed_hashes = set()

    try:
        for item in items:
            record = get_data(driver, item["url"])
            if not record:
                continue

            record["article_id"] = item["url_hash"]

            send_with_retry(record)
            producer.poll(0)

            processed_hashes.add(item["url_hash"])

    finally:
        driver.quit()
        producer.flush()

    return processed_hashes


# ================= MAIN =================
def main():
    try:
        items = list(load_url(CSV_FILE))
        if not items:
            return

        processed_hashes = crawl_to_kafka(items)

        if processed_hashes:
            mark_processed(CSV_FILE, processed_hashes)
            print(f"[INFO] Marked {len(processed_hashes)} URLs as processed.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()