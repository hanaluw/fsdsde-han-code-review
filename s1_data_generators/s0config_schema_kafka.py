from pathlib import Path

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "news_topic"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
DLQ_TOPIC = "news_dlq"
MAX_RETRY = 3

CSV_FILE = Path(__file__).parent / "output/cafef_articles/articles.csv"
BASE_URL = "https://cafef.vn"

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