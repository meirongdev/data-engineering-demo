"""Bronze ingest #2: pageview JSON on SeaweedFS -> demo.bronze.pageviews.

Reads the newline-delimited JSON the loadgen dropped in the `pageviews` bucket
over the s3a:// filesystem (hadoop-aws), then fully overwrites bronze.pageviews.
`received_at` arrives as epoch seconds; casting a long to timestamp interprets
it as seconds since epoch.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PAGEVIEWS_PATH = "s3a://pageviews/"

spark = SparkSession.builder.appName("s3-to-bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Reading pageview events from {PAGEVIEWS_PATH} ...")
raw = spark.read.json(PAGEVIEWS_PATH)

if len(raw.columns) == 0:
    print("No pageview data found — did the loadgen Job run? (make pipeline runs it)")
    spark.stop()
    sys.exit(1)

pageviews = raw.select(
    col("user_id").cast("long"),
    col("url").cast("string"),
    col("channel").cast("string"),
    col("received_at").cast("timestamp"),
)

pageviews.write.format("iceberg").mode("overwrite").save("demo.bronze.pageviews")
print(f"demo.bronze.pageviews: {pageviews.count()} rows")

spark.stop()
