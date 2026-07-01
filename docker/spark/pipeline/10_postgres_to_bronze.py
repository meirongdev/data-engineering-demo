"""Bronze ingest #1: Postgres (oneshop) -> demo.bronze.{users,items,purchases}.

Reads each OLTP table over JDBC and fully overwrites the matching bronze Iceberg
table. Static-mode `overwrite` replaces the whole table, keeping the reload
clean and repeatable for this bounded demo.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

POSTGRES_URL = "jdbc:postgresql://postgres:5432/oneshop"
JDBC_OPTS = {
    "url": POSTGRES_URL,
    "user": "etluser",
    "password": "etlpassword",
    "driver": "org.postgresql.Driver",
}

spark = SparkSession.builder.appName("postgres-to-bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_table(name):
    return spark.read.format("jdbc").options(**JDBC_OPTS).option("dbtable", name).load()


def overwrite(df, table):
    df.write.format("iceberg").mode("overwrite").save(table)
    print(f"{table}: {df.count()} rows")


# 1. users
overwrite(
    read_table("users").select(
        col("id").cast("long"),
        col("first_name").cast("string"),
        col("last_name").cast("string"),
        col("email").cast("string"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp"),
    ),
    "demo.bronze.users",
)

# 2. items
overwrite(
    read_table("items").select(
        col("id").cast("long"),
        col("name").cast("string"),
        col("category").cast("string"),
        col("price").cast("decimal(7,2)"),
        col("inventory").cast("int"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp"),
    ),
    "demo.bronze.items",
)

# 3. purchases
overwrite(
    read_table("purchases").select(
        col("id").cast("long"),
        col("user_id").cast("long"),
        col("item_id").cast("long"),
        col("quantity").cast("int"),
        col("purchase_price").cast("decimal(12,2)"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp"),
    ),
    "demo.bronze.purchases",
)

spark.stop()
