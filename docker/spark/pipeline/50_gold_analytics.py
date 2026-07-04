"""Extended gold analytics — 4 new tables + CSV export to SeaweedFS.

  * top_selling_items          — top 10 items by revenue
  * sales_performance_24h      — hourly revenue over the last 24 hours
  * pageviews_by_channel       — pageview count per channel
  * user_engagement_segments   — RFM-style user segmentation
  * CSV export to s3a://customer-segments/segmented_users

Every Iceberg write uses .format("iceberg") — see plan review note.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, current_date, current_timestamp,
    datediff, expr, lit, max, sum, to_date, when,
)

spark = SparkSession.builder.appName("gold-analytics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- 1. top_selling_items ----------------------------------------------------
top_items = (
    spark.table("demo.silver.purchases_enriched")
    .groupBy("item_id", "item_name", "item_category")
    .agg(sum("total_price").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
    .limit(10)
)
top_items.write.format("iceberg").mode("overwrite").save("demo.gold.top_selling_items")
print(f"demo.gold.top_selling_items: {top_items.count()} rows")
top_items.show(10, truncate=False)

# --- 2. sales_performance_24h ------------------------------------------------
perf_24h = (
    spark.table("demo.silver.purchases_enriched")
    .filter(col("created_at") >= current_timestamp() - expr("INTERVAL 24 HOURS"))
    .groupBy("purchase_hour")
    .agg(sum("total_price").alias("total_revenue"))
    .orderBy("purchase_hour")
)
perf_24h.write.format("iceberg").mode("overwrite").save("demo.gold.sales_performance_24h")
print(f"demo.gold.sales_performance_24h: {perf_24h.count()} rows")
perf_24h.show(24, truncate=False)

# --- 3. pageviews_by_channel -------------------------------------------------
pvs_by_channel = (
    spark.table("demo.silver.pageviews_by_items")
    .groupBy("channel")
    .agg(count(lit(1)).alias("total_pageviews"))
    .orderBy(col("total_pageviews").desc())
)
pvs_by_channel.write.format("iceberg").mode("overwrite").save("demo.gold.pageviews_by_channel")
print(f"demo.gold.pageviews_by_channel: {pvs_by_channel.count()} rows")
pvs_by_channel.show(10, truncate=False)

# --- 4. user_engagement_segments (RFM-style) ---------------------------------
# Join on users.id → user_id (see plan: silver.users keys on `id`, not `user_id`)
users = spark.table("demo.silver.users").select(
    col("id").alias("user_id"), "email", "full_name", "valid_email"
)
pvs = spark.table("demo.silver.pageviews_by_items")

segments = (
    users.join(pvs, "user_id", "left")
    .filter(col("valid_email") == True)
    .groupBy("user_id", "email", "full_name")
    .agg(
        count("page").alias("total_pageviews"),
        countDistinct(to_date(col("received_at"))).alias("active_days"),
        max(to_date(col("received_at"))).alias("last_active_date"),
    )
    .withColumn(
        "days_since_last_active",
        datediff(current_date(), col("last_active_date")),
    )
    .withColumn(
        "engagement_segment",
        when(
            (col("total_pageviews") >= 8) & (col("days_since_last_active") <= 3),
            lit("high_engagement"),
        )
        .when(
            (col("total_pageviews") >= 3) & (col("days_since_last_active") <= 7),
            lit("medium_engagement"),
        )
        .otherwise(lit("low_engagement")),
    )
)
segments.write.format("iceberg").mode("overwrite").save("demo.gold.user_engagement_segments")
print(f"demo.gold.user_engagement_segments: {segments.count()} rows")
segments.groupBy("engagement_segment").count().show(10, truncate=False)

# --- 5. CSV export to SeaweedFS (for downstream / marketing team) ------------
segments.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "s3a://customer-segments/segmented_users"
)
print("CSV exported to s3a://customer-segments/segmented_users/")

spark.stop()
