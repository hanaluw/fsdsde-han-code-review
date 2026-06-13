from s0config_schema_kafka import CSV_FILE
from s2scraper import create_driver, get_data, load_url, mark_processed
from s1news_producer import create_producer, send_with_retry

def task_load_urls(**context):
    context["ti"].xcom_push(key="csv_path", value=str(CSV_FILE))
    print("[INFO] CSV path pushed.")

def task_crawl_and_send(**context):
    csv_file = context["ti"].xcom_pull(key="csv_path", task_ids="load_urls")
    if not csv_file:
        return

    producer, avro_serializer = create_producer()
    processed_hashes = []
    driver = create_driver()

    try:
        for item in load_url(csv_file):
            record = get_data(driver, item["url"])
            if not record:
                continue

            record["article_id"] = item["url_hash"]

            send_with_retry(producer, avro_serializer, record)
            producer.poll(0)

            processed_hashes.append(item["url_hash"])

    finally:
        driver.quit()
        producer.flush()

    context["ti"].xcom_push(key="processed_hashes", value=processed_hashes)
    print(f"[INFO] Sent {len(processed_hashes)} records.")

def task_mark_processed(**context):
    processed_hashes = context["ti"].xcom_pull(
        key="processed_hashes",
        task_ids="crawl_and_send"
    )

    if not processed_hashes:
        return

    mark_processed(CSV_FILE, set(processed_hashes))
    print(f"[INFO] Marked {len(processed_hashes)} URLs as processed.")

def main():
    print("START MAIN")
    producer, avro_serializer = create_producer()
    processed_hashes = set()
    driver = create_driver()
    items = list(load_url(CSV_FILE))
    print(f"[INFO] Loaded {len(items)} URLs to process.")

    try:
        for i, item in enumerate(items, 1):
            print(f"[{i}/{len(items)}] Crawling: {item['url']}")
            record = get_data(driver, item["url"])
            if not record:
                print(f"  [SKIP] No data returned for {item['url']}")
                continue
            record["article_id"] = item["url_hash"]
            send_with_retry(producer, avro_serializer, record)
            producer.poll(0)  

            processed_hashes.add(item["url_hash"])
            print(f"  [SENT] {item['url']}")

    finally:
        driver.quit()
        producer.flush()
    if processed_hashes:
        mark_processed(CSV_FILE, processed_hashes)
        print(f"[INFO] Marked {len(processed_hashes)} URLs as processed.")
    else:
        print("[WARN] No URLs were successfully processed.")

if __name__ == "__main__":
    main()