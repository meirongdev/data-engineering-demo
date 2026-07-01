"""Create the medallion namespaces and Iceberg tables in the `demo` catalog.

Bronze = raw copies of the sources; Silver = validated + enriched; Gold =
analytics-ready. The `demo` REST catalog is the default (see spark-defaults.conf),
so `demo.bronze.users` etc. resolve straight to SeaweedFS-backed Iceberg tables.

Idempotent: everything is CREATE ... IF NOT EXISTS.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create-tables").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for ns in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS demo.{ns}")

DDL = [
    # --- bronze: raw ingest ---------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS demo.bronze.users (
        id BIGINT, first_name STRING, last_name STRING, email STRING,
        created_at TIMESTAMP, updated_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(created_at))
    TBLPROPERTIES ('comment' = 'Raw user dimension from Postgres')
    """,
    """
    CREATE TABLE IF NOT EXISTS demo.bronze.items (
        id BIGINT, name STRING, category STRING, price DECIMAL(7,2),
        inventory INT, created_at TIMESTAMP, updated_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (category)
    TBLPROPERTIES ('comment' = 'Raw item dimension from Postgres')
    """,
    """
    CREATE TABLE IF NOT EXISTS demo.bronze.purchases (
        id BIGINT, user_id BIGINT, item_id BIGINT, quantity INT,
        purchase_price DECIMAL(12,2), created_at TIMESTAMP, updated_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(created_at))
    TBLPROPERTIES ('comment' = 'Raw purchase facts from Postgres')
    """,
    """
    CREATE TABLE IF NOT EXISTS demo.bronze.pageviews (
        user_id BIGINT, url STRING, channel STRING, received_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(received_at))
    TBLPROPERTIES ('comment' = 'Raw pageview clickstream from SeaweedFS JSON')
    """,
    # --- silver: validated + enriched -----------------------------------------
    """
    CREATE TABLE IF NOT EXISTS demo.silver.users (
        id BIGINT, first_name STRING, last_name STRING, email STRING,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        valid_email BOOLEAN, full_name STRING
    ) USING iceberg PARTITIONED BY (days(created_at))
    TBLPROPERTIES ('comment' = 'Validated user dimension')
    """,
    """
    CREATE TABLE IF NOT EXISTS demo.silver.items (
        id BIGINT, name STRING, category STRING, price DECIMAL(7,2),
        inventory INT, created_at TIMESTAMP, updated_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (category)
    TBLPROPERTIES ('comment' = 'Cleaned item dimension')
    """,
    """
    CREATE TABLE IF NOT EXISTS demo.silver.purchases_enriched (
        id BIGINT, user_id BIGINT, item_id BIGINT, quantity INT,
        purchase_price DECIMAL(12,2), total_price DECIMAL(14,2),
        user_email STRING, item_name STRING, item_category STRING,
        purchase_date DATE, purchase_hour INT,
        created_at TIMESTAMP, updated_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(created_at))
    TBLPROPERTIES ('comment' = 'Purchases joined to users + items')
    """,
    """
    CREATE TABLE IF NOT EXISTS demo.silver.pageviews_by_items (
        user_id BIGINT, item_id BIGINT, page STRING,
        item_name STRING, item_category STRING, channel STRING,
        received_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(received_at))
    TBLPROPERTIES ('comment' = 'Pageviews parsed + joined to items')
    """,
    # --- gold: analytics ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS demo.gold.item_performance (
        item_id BIGINT, item_name STRING, item_category STRING,
        items_sold BIGINT, orders BIGINT, revenue DECIMAL(20,2),
        pageviews BIGINT, conversion_rate DOUBLE
    ) USING iceberg PARTITIONED BY (item_category)
    TBLPROPERTIES ('comment' = 'Per-item revenue, traffic and conversion')
    """,
]

for stmt in DDL:
    spark.sql(stmt)

print("Namespaces + tables ready:")
for ns in ("bronze", "silver", "gold"):
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN demo.{ns}").collect()]
    print(f"  demo.{ns}: {tables}")

spark.stop()
