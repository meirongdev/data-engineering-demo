"""Gold analytics: demo.gold.item_performance.

One row per item combining sales and traffic:
  items_sold, orders, revenue (from silver.purchases_enriched),
  pageviews      (from silver.pageviews_by_items, product pages only),
  conversion_rate = orders / pageviews.

Anchored on silver.items so every catalogued item appears, even with zero
sales or traffic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, count, lit, sum, when

spark = SparkSession.builder.appName("silver-to-gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

items = spark.table("demo.silver.items")
purchases = spark.table("demo.silver.purchases_enriched")
pageviews = spark.table("demo.silver.pageviews_by_items")

purchase_agg = purchases.groupBy("item_id").agg(
    sum("quantity").alias("items_sold"),
    count("id").alias("orders"),
    sum("total_price").alias("revenue"),
)

pageview_agg = (
    pageviews.filter(col("page") == "products")
    .groupBy("item_id")
    .agg(count(lit(1)).alias("pageviews"))
)

gold = (
    items.select(
        col("id").alias("item_id"),
        col("name").alias("item_name"),
        col("category").alias("item_category"),
    )
    .join(purchase_agg, "item_id", "left")
    .join(pageview_agg, "item_id", "left")
    .select(
        col("item_id"),
        col("item_name"),
        col("item_category"),
        coalesce(col("items_sold"), lit(0)).cast("long").alias("items_sold"),
        coalesce(col("orders"), lit(0)).cast("long").alias("orders"),
        coalesce(col("revenue"), lit(0)).cast("decimal(20,2)").alias("revenue"),
        coalesce(col("pageviews"), lit(0)).cast("long").alias("pageviews"),
        when(
            coalesce(col("pageviews"), lit(0)) > 0,
            col("orders") / col("pageviews"),
        )
        .otherwise(lit(0.0))
        .cast("double")
        .alias("conversion_rate"),
    )
)

gold.write.format("iceberg").mode("overwrite").save("demo.gold.item_performance")
print(f"demo.gold.item_performance: {gold.count()} rows")

print("Top 10 items by revenue:")
(
    gold.orderBy(col("revenue").desc())
    .select("item_name", "item_category", "orders", "revenue", "pageviews", "conversion_rate")
    .show(10, truncate=False)
)

spark.stop()
