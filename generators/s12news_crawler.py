import csv
import json
import hashlib
import threading
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import time
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CSV_FILE        = "output/cafef_articles/articles.csv"
BASE_URL        = "https://cafef.vn"
BATCH_SIZE      = 10
MAX_WORKERS     = 4

# File JSON dùng chung với generate_data.py
ARTICLE_IDS_OUT = "spark_demo_data/article_ids.json"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "news_db",
    "user":     "postgres",
    "password": "postgres",
}

_csv_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def create_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option(
        "prefs", {"profile.managed_default_content_settings.images": 2}
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# ---------------------------------------------------------------------------
# Load pending URLs — row-by-row, không load toàn bộ file vào RAM
# ---------------------------------------------------------------------------
def load_url(csv_file: str) -> list[dict]:
    pending = []
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"].strip().lower() == "processed":
                continue
            pending.append({
                "url":          row["url"].strip(),
                "url_hash":     row["url_hash"].strip(),
                "published_dt": row.get("published_dt", ""),
            })
    pending.sort(key=lambda r: r["published_dt"], reverse=True)
    return pending


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------
def get_data(driver, url: str) -> dict | None:
    try:
        driver.get(url)
        time.sleep(1.5)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        title_tag   = soup.find("title")
        pub_tag     = soup.find("span", {"data-role": "publishdate"})
        cate_tag    = soup.find("a",    {"data-role": "cate-name"})
        snippet_tag = soup.find("p",    {"data-role": "sapo"})
        tag_div     = soup.find("div",  {"data-marked-zoneid": "cafef_detail_tag"})

        return {
            "url":          url,
            "article_id":   hashlib.sha256(url.lower().encode()).hexdigest(),
            "title":        title_tag.text.strip() if title_tag else None,
            "pub_date":     pub_tag.get_text(strip=True) if pub_tag else None,
            "cate_source":  cate_tag.get_text(strip=True) if cate_tag else None,
            "snippet":      snippet_tag.get_text(strip=True) if snippet_tag else None,
            "key_words":    json.dumps(
                                [a.get_text(strip=True) for a in tag_div.find_all("a")]
                                if tag_div else [], ensure_ascii=False),
            "related_news": json.dumps([
                                {"title": li.a["title"].strip(),
                                 "link":  urljoin(BASE_URL, li.a["href"])}
                                for li in soup.select("ul.tinlienquan li")
                                if li.a and li.a.get("href") and li.a.get("title")
                            ], ensure_ascii=False),
            "crawled_at":   datetime.utcnow().isoformat(),
        }
    except Exception:
        return None


def crawl_one(item: dict) -> dict | None:
    driver = create_driver()
    try:
        print(f"  [crawl] {item['url']}")
        record = get_data(driver, item["url"])
        if record:
            record["article_id"] = item["url_hash"]
        return record
    finally:
        driver.quit()


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def save_to_postgres(records: list[dict]) -> list[str]:
    if not records:
        return []
    insert_sql = """
        INSERT INTO news_articles (
            article_id, url, title, pub_date, cate_source,
            snippet, key_words, related_news, crawled_at
        ) VALUES (
            %(article_id)s, %(url)s, %(title)s, %(pub_date)s, %(cate_source)s,
            %(snippet)s, %(key_words)s::jsonb, %(related_news)s::jsonb, %(crawled_at)s
        )
        ON CONFLICT (article_id) DO NOTHING;
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, records)
        saved = [r["article_id"] for r in records]
        print(f"  [DB] Saved {len(saved)} rows.")
        return saved
    except Exception as e:
        print(f"  [ERROR] DB insert failed: {e}")
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CSV update — thread-safe
# ---------------------------------------------------------------------------
def mark_as_read(csv_file: str, url_hash_list: list[str]) -> None:
    if not url_hash_list:
        return
    url_hash_set = set(url_hash_list)
    csv_path = Path(csv_file)
    tmp_path = csv_path.with_suffix(".tmp")
    with _csv_lock:
        with open(csv_path, "r", newline="", encoding="utf-8") as fin, \
             open(tmp_path, "w", newline="", encoding="utf-8") as fout:
            reader = csv.DictReader(fin)
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if row["url_hash"] in url_hash_set:
                    row["status"]       = "PROCESSED"
                    row["processed_at"] = datetime.utcnow().isoformat()
                writer.writerow(row)
        tmp_path.replace(csv_path)


# ---------------------------------------------------------------------------
# Export article_ids → JSON (dùng cho generate_data.py)
# Đọc CSV 1 lần row-by-row, chỉ lấy cột url_hash của PROCESSED rows
# Không load DB, không giữ record đầy đủ trong RAM
# ---------------------------------------------------------------------------
def export_article_ids(csv_file: str, out_path: str = ARTICLE_IDS_OUT) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(csv_file, newline="", encoding="utf-8") as f, \
         open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[")
        first = True
        for row in csv.DictReader(f):
            if row["status"].strip().lower() != "processed":
                continue
            if not first:
                fout.write(",")
            fout.write(json.dumps(row["url_hash"].strip()))
            first = False
        fout.write("]")
    print(f"  [export] article_ids → {out_path}")


# ---------------------------------------------------------------------------
# DB init
# ---------------------------------------------------------------------------
def create_database_if_not_exists() -> None:
    conn = psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        dbname="postgres", user=DB_CONFIG["user"], password=DB_CONFIG["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'news_db'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE news_db")
        print("  [DB] Database 'news_db' created.")
    else:
        print("  [DB] Database 'news_db' already exists.")
    cur.close(); conn.close()


def initialize_db() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            article_id   VARCHAR(64)  PRIMARY KEY,
            url          TEXT         NOT NULL,
            title        TEXT,
            pub_date     VARCHAR(50),
            cate_source  TEXT,
            snippet      TEXT,
            key_words    JSONB,
            related_news JSONB,
            crawled_at   TIMESTAMP
        );
    """)
    print("  [DB] Table ready.")
    cur.close(); conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    create_database_if_not_exists()
    initialize_db()

    pending = load_url(CSV_FILE)
    if not pending:
        print("No pending URLs. Exiting.")
        return
    print(f"Found {len(pending)} pending URLs. Running {MAX_WORKERS} workers.\n")

    batch_records: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(crawl_one, item): item for item in pending}
        for future in as_completed(futures):
            record = future.result()
            if record is None:
                continue
            batch_records.append(record)
            if len(batch_records) >= BATCH_SIZE:
                saved = save_to_postgres(batch_records)
                mark_as_read(CSV_FILE, saved)
                batch_records.clear()

    if batch_records:
        saved = save_to_postgres(batch_records)
        mark_as_read(CSV_FILE, saved)

    # Export sau khi toàn bộ PROCESSED — đọc lại CSV 1 lần, stream ra file
    export_article_ids(CSV_FILE)
    print("\nDone.")


if __name__ == "__main__":
    main()