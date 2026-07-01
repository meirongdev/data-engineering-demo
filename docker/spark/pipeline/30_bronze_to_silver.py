"""Silver transform: validate + enrich the bronze tables.

  * silver.users              — add valid_email (regex) + full_name
  * silver.items              — clamp negative prices, upper-case category
  * silver.purchases_enriched — join to users + items; derive total_price,
                                purchase_date, purchase_hour
  * silver.pageviews_by_items — parse "/products/{id}" URLs, join to items
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    hour,
    lit,
    regexp_extract,
    to_date,
    upper,
    when,
)

spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

users = spark.table("demo.bronze.users")
items = spark.table("demo.bronze.items")
purchases = spark.table("demo.bronze.purchases")
pageviews = spark.table("demo.bronze.pageviews")

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def overwrite(df, table):
    df.write.format("iceberg").mode("overwrite").save(table)
    print(f"{table}: {df.count()} rows")


# --- silver.users -------------------------------------------------------------
overwrite(
    users.withColumn("valid_email", col("email").rlike(EMAIL_REGEX))
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
    .select(
        "id", "first_name", "last_name", "email",
        "created_at", "updated_at", "valid_email", "full_name",
    ),
    "demo.silver.users",
)

# --- silver.items -------------------------------------------------------------
overwrite(
    items.withColumn("price", when(col("price") < 0, lit(0)).otherwise(col("price")))
    .withColumn("category", upper(col("category")))
    .select("id", "name", "category", "price", "inventory", "created_at", "updated_at"),
    "demo.silver.items",
)

# --- silver.purchases_enriched ------------------------------------------------
overwrite(
    purchases.alias("p")
    .join(users.alias("u"), col("p.user_id") == col("u.id"), "left")
    .join(items.alias("i"), col("p.item_id") == col("i.id"), "left")
    .select(
        col("p.id"),
        col("p.user_id"),
        col("p.item_id"),
        col("p.quantity"),
        col("p.purchase_price"),
        (col("p.quantity") * col("p.purchase_price")).cast("decimal(14,2)").alias("total_price"),
        col("u.email").alias("user_email"),
        col("i.name").alias("item_name"),
        col("i.category").alias("item_category"),
        to_date(col("p.created_at")).alias("purchase_date"),
        hour(col("p.created_at")).alias("purchase_hour"),
        col("p.created_at"),
        col("p.updated_at"),
    ),
    "demo.silver.purchases_enriched",
)

# --- silver.pageviews_by_items ------------------------------------------------
# URL shape is "/{page}/{item_id}", e.g. "/products/42".
parsed = (
    pageviews.withColumn("page", regexp_extract(col("url"), r"^/([^/]+)/\d+$", 1))
    .withColumn("item_id", regexp_extract(col("url"), r"/(\d+)$", 1).cast("bigint"))
    .filter(col("item_id").isNotNull())
)
overwrite(
    parsed.alias("v")
    .join(items.alias("i"), col("v.item_id") == col("i.id"), "left")
    .select(
        col("v.user_id"),
        col("v.item_id"),
        col("v.page"),
        col("i.name").alias("item_name"),
        col("i.category").alias("item_category"),
        col("v.channel"),
        col("v.received_at"),
    ),
    "demo.silver.pageviews_by_items",
)

spark.stop()
