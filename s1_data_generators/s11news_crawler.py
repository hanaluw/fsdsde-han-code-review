import csv
from datetime import datetime
import hashlib
import time
import pandas as pd
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

#  Constants 

URLS = [
    "https://cafef.vn/smart-money.chn",
    "https://cafef.vn/kinh-te-so.chn",
]
OUTPUT_FILE   = Path("output/cafef_articles/articles.csv")
CHECKPOINT_EVERY = 1
ARTICLE_CSS   = ".list-news-main.top5_news .tlitem.box-category-item"
SEE_MORE_CSS  = "div.btn-viewmore"


#  Driver setup / teardown 

def build_driver() -> webdriver.Chrome:
    """Create and return a headless Chrome driver."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs",prefs)

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


#  Page interaction helpers 

def scroll_to_bottom(driver: webdriver.Chrome) -> None:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(2)


def click_see_more(driver: webdriver.Chrome) -> bool:
    """Click the 'see more' button. Returns False if the button is not found."""
    try:
        btn = driver.find_element(By.CSS_SELECTOR, SEE_MORE_CSS)
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
        return True
    except Exception:
        return False


#  Article parsing 

def parse_article(element) -> dict:
    """Extract fields from a single article WebElement."""
    url          = element.find_element(By.CSS_SELECTOR, "h3 a").get_attribute("href")
    title        = element.find_element(By.CSS_SELECTOR, "h3 a").text
    date_str     = element.find_element(By.CSS_SELECTOR, ".time.time-ago").get_attribute("title")
    published_dt = datetime.fromisoformat(date_str)
    discovered_at = datetime.utcnow()
    url_hash = hashlib.sha256(url.lower().encode("utf-8")).hexdigest()

    return {
        # "title":        title,
        "url":          url,
        "published_dt": published_dt,
        "year":         published_dt.year,
        "month":        published_dt.month,
        # tracking
        "discovered_at": discovered_at,
        "url_hash": url_hash,
        # orchestration metadata
        "status":       "NEW",
        "processed_at": None,
        "retry_count":  0,
    }


def collect_articles_from_page(driver, seen: set) -> tuple[list[dict], bool]:
    new_articles  = []
    stop_crawling = False
    TARGET_YEAR   = 2026

    for element in driver.find_elements(By.CSS_SELECTOR, ARTICLE_CSS):
        article = parse_article(element)

        if article["url"] in seen:
            continue

        seen.add(article["url"])
        new_articles.append(article)
        print(f"New: {article['published_dt'].date()} -- {article['url']}")

    # Chỉ check year trên bài MỚI của batch này
    if new_articles:
        oldest_year = min(a["published_dt"].year for a in new_articles)
        if oldest_year < TARGET_YEAR:
            stop_crawling = True

    return new_articles, stop_crawling


#  Save All Articles to CSV 

def save_to_csv(articles: list[dict], output_file: Path) -> None:
    if not articles:
        print("No articles to save.")
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)

    file_exists = output_file.exists()

    with open(output_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=articles[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(articles)
    print(f"\nAppended {len(articles)} articles to {output_file}")

#  Load existing URLs from CSV to avoid duplicates
def load_existing_urls_from_csv(
    output_file: Path) -> set[str]:
    if not output_file.exists():
        return set()
    
    try:
        df = pd.read_csv(output_file, usecols=["url"])
        urls = set(df["url"].dropna().astype(str))
        return urls

    except Exception as e:
        print(f"Error reading existing URLs from {output_file}: {e}")
        return set()

#  Entry point
def crawl_one_url(driver, url: str, seen: set) -> None:
    """Crawl a single listing URL to exhaustion, updating seen in-place."""
    print(f"\n{'='*60}\nCrawling: {url}\n{'='*60}")
    pending = []
    new_articles_count = 0
    click_no = 0

    driver.get(url)
    time.sleep(3)

    try:
        while True:
            click_no += 1
            scroll_to_bottom(driver)
            click_see_more(driver)


            # if not click_see_more(driver):
                # continue
            # print(f"\nBatch {click_no}")

            new_articles, stop_crawling = collect_articles_from_page(driver, seen)
            if not new_articles:
                print("No more news. I'm out. Peace!")
                break

            pending.extend(new_articles)
            new_articles_count += len(new_articles)
            print(f"Found {len(new_articles)} new articles | Run total: {new_articles_count}")

            if pending:
                save_to_csv(pending, OUTPUT_FILE)
                pending.clear()

            if stop_crawling:
                break
    finally:
        if pending:
            save_to_csv(pending, OUTPUT_FILE)


def main():
    seen   = load_existing_urls_from_csv(OUTPUT_FILE)
    driver = build_driver()
    try:
        for url in URLS:
            crawl_one_url(driver, url, seen)
    finally:
        driver.quit()
    print("\nAll URLs done.")


if __name__ == "__main__":
    main()


# clean up task: rm -f ~/.wdm/.wdm-lock-chromedriver-linux64