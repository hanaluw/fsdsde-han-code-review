import random
import psycopg2
from datetime import datetime, timezone, timedelta

random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
NUM_USERS = 50_000

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "news_db",
    "user":     "postgres",
    "password": "postgres",
}

SIGNUP_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
SIGNUP_END   = datetime.now(timezone.utc)
SIGNUP_RANGE_DAYS = (SIGNUP_END - SIGNUP_START).days


# ── Connect & init table ──────────────────────────────────────────────────────
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id            VARCHAR(20)  PRIMARY KEY,
        signup_ts          TIMESTAMP    NOT NULL,
        email_subscription BOOLEAN      NOT NULL
    )
""")
print("  [DB] Table 'users' ready.")


# ── Generate users ────────────────────────────────────────────────────────────
def random_signup_ts() -> datetime:
    offset_days    = random.randint(0, SIGNUP_RANGE_DAYS)
    offset_seconds = random.randint(0, 86400)
    return SIGNUP_START + timedelta(days=offset_days, seconds=offset_seconds)


rows = []
for i in range(NUM_USERS):
    user_id  = f"user_{i+1:06d}"
    signup   = random_signup_ts()
    subscribed = random.random() < 0.7
    rows.append((user_id, signup, subscribed))
    print(f"  [GEN] {user_id} | signup={signup.date()} | subscribed={subscribed}")


# ── Batch insert ──────────────────────────────────────────────────────────────
cursor.executemany(
    "INSERT INTO users (user_id, signup_ts, email_subscription) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
    rows,
)
print(f"  [DB] Inserted {NUM_USERS} users into news_db.public.users.")

cursor.execute("SELECT COUNT(*) FROM users")
print(f"  [DB] Total rows in users table: {cursor.fetchone()[0]}")

cursor.close()
conn.close()