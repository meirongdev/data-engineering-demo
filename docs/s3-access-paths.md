# S3 access paths: S3FileIO vs. Hadoop s3a

This lab talks to SeaweedFS through **two independent S3 mechanisms** that
run in the same Spark image. They use different AWS SDK versions, different
configuration keys, and serve different purposes. This page explains why both
exist and how they coexist.

## The two paths in one diagram

```
                         SeaweedFS (:8333)
                             │
              ┌──────────────┴──────────────┐
              │                             │
      Iceberg S3FileIO               Hadoop s3a
      (AWS SDK v2)                   (AWS SDK v1)
              │                             │
   Iceberg table I/O              Generic file I/O
   (Parquet + metadata)          (raw JSON, CSV, etc.)
              │                             │
     spark.sql.catalog.           spark.hadoop.fs.s3a.*
     demo.s3.* (config)           (config)
              │                             │
         All tables:               Pipeline only:
   bronze / silver / gold        pageviews bucket read
   ┌─────────────────────┐     ┌────────────────────┐
   │ spark.read.format    │     │ spark.read.json(    │
   │   ("iceberg") ...    │     │   "s3a://pageviews/")│
   │ spark.sql("SELECT")  │     └────────────────────┘
   └─────────────────────┘
```

## Path 1: Iceberg S3FileIO (AWS SDK v2)

**What it does:** Reads and writes Iceberg table data files (Parquet) and
Iceberg metadata files (JSON, Avro) on object storage.

**How it's loaded:** `iceberg-aws-bundle-1.10.1.jar` — a shaded jar that
bundles Iceberg's `S3FileIO` class and the AWS SDK v2 classes. It is placed in
`${SPARK_HOME}/jars/` at build time (see [docker-images.md](docker-images.md)).

**How it's configured:** In `spark-defaults.conf`, under the `demo` catalog's
`spark.sql.catalog.demo.s3.*` prefix:

```
spark.sql.catalog.demo.io-impl        org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.demo.s3.endpoint    http://seaweedfs:8333
spark.sql.catalog.demo.s3.path-style-access  true
spark.sql.catalog.demo.s3.access-key-id      admin
spark.sql.catalog.demo.s3.secret-access-key  password
```

These settings are specific to Iceberg's `S3FileIO` — they do not affect
Spark's generic Hadoop filesystem at all.

**When it's used:** Any time Spark touches an Iceberg table — all pipeline
stages (`10_postgres_to_bronze.py` through `40_silver_to_gold.py`), smoke
tests, and notebook SQL queries. The catalog returns an `s3://warehouse/` path
and `S3FileIO` handles the actual data transfer.

## Path 2: Hadoop s3a (AWS SDK v1)

**What it does:** A Hadoop `FileSystem` implementation that allows Spark to
read/write **arbitrary files** on S3-compatible storage — not just Iceberg
tables.

**How it's loaded:** Two jars placed in `${SPARK_HOME}/jars/` at build time:

| Jar | Role | Version constraint |
|---|---|---|
| `hadoop-aws-3.3.4.jar` | Implements the `s3a://` scheme | Must match Hadoop version in the Spark build (3.3.4 for Spark 3.5.x bin-hadoop3) |
| `aws-java-sdk-bundle-1.12.262.jar` | AWS SDK v1 classes | Must match the version `hadoop-aws` was compiled against |

**How it's configured:** In `spark-defaults.conf`, under the
`spark.hadoop.fs.s3a.*` prefix:

```
spark.hadoop.fs.s3a.endpoint        http://seaweedfs:8333
spark.hadoop.fs.s3a.access.key      admin
spark.hadoop.fs.s3a.secret.key      password
spark.hadoop.fs.s3a.path.style.access   true
spark.hadoop.fs.s3a.connection.ssl.enabled  false
spark.hadoop.fs.s3a.impl            org.apache.hadoop.fs.s3a.S3AFileSystem
```

The `spark.hadoop.*` prefix means "pass these as Hadoop configuration values".
They only affect the Hadoop `FileSystem` layer — not Iceberg's `S3FileIO`.

**When it's used:** Only by `20_s3_to_bronze.py`, which reads raw JSON files
from the `pageviews` bucket:

```python
PAGEVIEWS_PATH = "s3a://pageviews/"
spark.read.json(PAGEVIEWS_PATH)
```

Without `hadoop-aws`, this would fail with
`No FileSystem for scheme "s3a"` — Spark would not know how to interpret an
`s3a://` URL.

## Why do they coexist?

The two paths exist because they solve different problems:

| | S3FileIO (Iceberg) | s3a (Hadoop) |
|---|---|---|
| Reads | Iceberg tables (Parquet + metadata) | Raw files (JSON, CSV, text) |
| SDK version | AWS SDK v2 (shaded in `iceberg-aws-bundle`) | AWS SDK v1 (shaded in `aws-java-sdk-bundle`) |
| Configuration | `catalog.s3.*` inside catalog properties | `fs.s3a.*` as Hadoop configuration |
| Version coupled to | Iceberg release | Hadoop release |
| Created by | Iceberg project | Hadoop project |
| Purpose | Iceberg table I/O only | Any file I/O on S3 |

**Spark cannot use `S3FileIO` for `spark.read.json("s3a://...")`.** The
`spark.read.json()` API goes through Hadoop's `FileSystem` abstraction, which
only knows about Hadoop filesystem implementations (HDFS, `s3a`, `wasbs`,
`abfss`, etc.). Iceberg's `S3FileIO` implements Iceberg's `FileIO` interface,
not Hadoop's — they are incompatible by design.

Conversely, **Hadoop's `s3a` is not used for Iceberg table reads.** Spark
talks to the catalog, which returns a metadata pointer, and Iceberg's own
`S3FileIO` fetches the Parquet files. The Hadoop file system is bypassed
entirely.

## Why two AWS SDK versions?

The two SDK versions are not a design choice — they follow from the fact that
each project bundles its own S3 client:

- **Hadoop s3a** was written when AWS SDK v1 was current. It depends on the
  v1 classes (`com.amazonaws.*`).
- **Iceberg's S3FileIO** uses AWS SDK v2 (`software.amazon.awssdk.*`) because
  that's what the Iceberg project chose for its modern S3 integration.

The class names are entirely different (different Java packages), so both SDK
versions can coexist in the same JVM classpath without conflict — as long as
they are both shaded (renamed) or the class versions happen not to collide.
The `iceberg-aws-bundle` jar shades its v2 classes, preventing version
conflicts with anything else on the classpath.

## What about boto3?

There is a third S3 client in the mix: the **loadgen** image uses **boto3**
(AWS SDK v3, Python) to write pageview JSON to the `pageviews` bucket:

```python
import boto3
s3 = boto3.client("s3",
    endpoint_url="http://seaweedfs:8333",
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    use_ssl=False,
    config=Config(s3={"addressing_style": "path"}))
s3.put_object(Bucket="pageviews", Key="batch-001.json", Body=json_data)
```

boto3 is the Python-native client and has no connection to either `S3FileIO`
or `s3a`. It is configured separately in `generate_load.py`.

## Key takeaways

- **S3FileIO** (`demo.s3.*`) → Iceberg table data. Used by all pipeline stages.
- **s3a** (`fs.s3a.*`) → raw JSON reads only. Used by `20_s3_to_bronze.py`.
- Both point at the same SeaweedFS endpoint, use the same credentials.
- Two SDK versions coexist in the same JVM because they have different class
  packages and the v2 classes are shaded.
- If either `iceberg-aws-bundle.jar` or `hadoop-aws.jar` is missing, the
  corresponding path fails — the other continues to work.

## See also

- [docker-images.md](docker-images.md) — where each jar is downloaded in the Dockerfile
- [configuration.md](configuration.md) — the full `spark-defaults.conf` and version pins
- [pipeline.md](pipeline.md) — the two bronze ingestion stages that use each path
- [troubleshooting.md](troubleshooting.md) — s3a `ClassNotFoundException`, S3 addressing errors
