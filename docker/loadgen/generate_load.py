"""One-shot load generator for the "oneshop" lakehouse demo.

Two sinks, mirroring a real ingestion setup:

  * Postgres  — the operational store. We seed `users`, `items` and
    `purchases` (read later over JDBC into `bronze.{users,items,purchases}`).
  * SeaweedFS — object storage. We write clickstream `pageviews` as
    newline-delimited JSON (read later via `s3a://` into `bronze.pageviews`).

It is deliberately bounded and idempotent: it TRUNCATEs the Postgres tables and
clears the pageviews bucket first, so re-running gives a clean, reproducible
dataset. Counts are configurable via env (see the CONFIG block).

Mirrors a typical Postgres + MinIO ingestion setup; here it targets
Postgres + SeaweedFS and batches pageviews into NDJSON objects instead of one
tiny object per event.
"""

import io
import json
import os
import random
import time

import boto3
import psycopg2
from botocore.client import Config
from botocore.exceptions import ClientError
from faker import Faker

# --- CONFIG (override via env) ------------------------------------------------
USER_COUNT = int(os.getenv("USER_COUNT", "1000"))
ITEM_COUNT = int(os.getenv("ITEM_COUNT", "200"))
PURCHASE_COUNT = int(os.getenv("PURCHASE_COUNT", "200"))
PAGEVIEWS_PER_PURCHASE = int(os.getenv("PAGEVIEWS_PER_PURCHASE", "30"))
PAGEVIEW_WINDOW_DAYS = int(os.getenv("PAGEVIEW_WINDOW_DAYS", "14"))  # scatter received_at for meaningful RFM recency
NULL_EMAIL_RATE = float(os.getenv("NULL_EMAIL_RATE", "0.1"))  # exercise silver validation

ITEM_PRICE_MIN = 5
ITEM_PRICE_MAX = 500
ITEM_INVENTORY_MIN = 1000
ITEM_INVENTORY_MAX = 5000

CHANNELS = ["organic search", "paid search", "referral", "social", "display"]
CATEGORIES = ["widgets", "gadgets", "doodads", "clearance"]

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    user=os.getenv("POSTGRES_USER", "postgresuser"),
    password=os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    dbname=os.getenv("POSTGRES_DB", "oneshop"),
)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
PAGEVIEWS_BUCKET = os.getenv("PAGEVIEWS_BUCKET", "pageviews")

fake = Faker()


# --- S3 (SeaweedFS) -----------------------------------------------------------
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def reset_bucket(s3):
    """Create the pageviews bucket if missing and empty it for a clean seed."""
    try:
        s3.head_bucket(Bucket=PAGEVIEWS_BUCKET)
    except ClientError:
        print(f"Creating bucket '{PAGEVIEWS_BUCKET}' ...")
        s3.create_bucket(Bucket=PAGEVIEWS_BUCKET)

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=PAGEVIEWS_BUCKET):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=PAGEVIEWS_BUCKET, Delete={"Objects": objs})
            deleted += len(objs)
    if deleted:
        print(f"Cleared {deleted} existing object(s) from '{PAGEVIEWS_BUCKET}'.")


def write_pageviews(s3, batch_index, events):
    """Write one batch of pageview events as a newline-delimited JSON object."""
    body = "\n".join(json.dumps(e) for e in events).encode("utf-8")
    key = f"batch_{batch_index:05d}.json"
    s3.put_object(
        Bucket=PAGEVIEWS_BUCKET,
        Key=key,
        Body=io.BytesIO(body),
        ContentLength=len(body),
        ContentType="application/x-ndjson",
    )


def pageview(viewer_id, item_id):
    # URL shape must stay "/products/{item_id}" — the silver transform parses it.
    # Scatter received_at over PAGEVIEW_WINDOW_DAYS so the RFM recency dimension
    # is meaningful and the days(received_at) partition is non-degenerate.
    return {
        "user_id": viewer_id,
        "url": f"/products/{item_id}",
        "channel": random.choice(CHANNELS),
        "received_at": int(time.time() - random.randint(0, PAGEVIEW_WINDOW_DAYS * 86400)),
    }


# --- main ---------------------------------------------------------------------
def main():
    s3 = s3_client()
    reset_bucket(s3)

    with psycopg2.connect(**PG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            print("Truncating oneshop tables for a clean seed ...")
            cur.execute("TRUNCATE purchases, items, users RESTART IDENTITY CASCADE")

            print(f"Seeding {ITEM_COUNT} items and {USER_COUNT} users ...")
            cur.executemany(
                "INSERT INTO items (name, category, price, inventory) VALUES (%s, %s, %s, %s)",
                [
                    (
                        fake.word().capitalize(),
                        random.choice(CATEGORIES),
                        random.randint(ITEM_PRICE_MIN * 100, ITEM_PRICE_MAX * 100) / 100,
                        random.randint(ITEM_INVENTORY_MIN, ITEM_INVENTORY_MAX),
                    )
                    for _ in range(ITEM_COUNT)
                ],
            )
            cur.executemany(
                "INSERT INTO users (first_name, last_name, email) VALUES (%s, %s, %s)",
                [
                    (
                        fake.first_name(),
                        fake.last_name(),
                        None if random.random() < NULL_EMAIL_RATE else fake.email(),
                    )
                    for _ in range(USER_COUNT)
                ],
            )
            conn.commit()

            cur.execute("SELECT id, price FROM items")
            items = cur.fetchall()  # [(id, price), ...]
            cur.execute("SELECT id FROM users")
            user_ids = [r[0] for r in cur.fetchall()]

            print(
                f"Generating {PURCHASE_COUNT} purchases + "
                f"~{PURCHASE_COUNT * PAGEVIEWS_PER_PURCHASE} pageviews ..."
            )
            purchase_sql = (
                "INSERT INTO purchases (user_id, item_id, quantity, purchase_price, created_at) "
                "VALUES (%s, %s, %s, %s, %s)"
            )
            for i in range(PURCHASE_COUNT):
                item_id, price = random.choice(items)
                buyer = random.choice(user_ids)
                qty = random.randint(1, 5)
                # Spread purchases over the last 24h so day-partitioning is exercised.
                created_at = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(time.time() - random.randint(0, 24 * 60 * 60)),
                )

                events = [pageview(buyer, item_id)]  # the buyer viewed the product
                events += [
                    pageview(random.choice(user_ids), random.choice(items)[0])
                    for _ in range(PAGEVIEWS_PER_PURCHASE)
                ]
                write_pageviews(s3, i, events)

                # purchase_price is the UNIT price at purchase time; the silver
                # layer derives total_price = quantity * purchase_price.
                cur.execute(purchase_sql, (buyer, item_id, qty, price, created_at))
                conn.commit()

    print("Load generation complete.")


if __name__ == "__main__":
    main()
