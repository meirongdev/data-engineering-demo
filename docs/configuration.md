# Configuration

## Versions

The image (`docker/spark/Dockerfile`) pins a battle-tested combination. The
Spark, Iceberg, and Scala versions are build `ARG`s — override them at build
time (`docker build --build-arg ICEBERG_VERSION=…`) or edit the defaults.

| Component | Version | Set in |
|---|---|---|
| Base image | `python:3.11-slim-bookworm` | Dockerfile `FROM` |
| Java | OpenJDK 17 (headless) | Dockerfile `apt-get` |
| Spark | 3.5.8 (`hadoop3` build) | `ARG SPARK_VERSION` |
| Iceberg (Spark runtime + AWS bundle) | 1.10.1 | `ARG ICEBERG_VERSION` |
| Scala | 2.12 | `ARG SCALA_VERSION` |
| Postgres JDBC driver | 42.7.6 | `ARG POSTGRES_JDBC_VERSION` |
| hadoop-aws (s3a://) | 3.3.4 | `ARG HADOOP_AWS_VERSION` |
| aws-java-sdk-bundle (s3a:// deps) | 1.12.262 | `ARG AWS_SDK_BUNDLE_VERSION` |
| PyIceberg | 0.11.1 (`[pyarrow,duckdb,pandas]`) | `pip install` |
| JupyterLab | 4.6.1 | `pip install` |
| pandas / prettytable / matplotlib | 2.3.3 / 3.18.0 / 3.11.0 | `pip install` |

`hadoop-aws` **must** match the Hadoop version bundled in the Spark
`bin-hadoop3` distribution (3.3.4 for Spark 3.5.x), and `aws-java-sdk-bundle`
must be the version that `hadoop-aws` was built against — otherwise `s3a://`
reads fail at runtime. These jars power raw-JSON reads for the pipeline and are
independent of Iceberg's `S3FileIO` (which uses the AWS SDK **v2** shaded into
`iceberg-aws-bundle`).

The loadgen image (`docker/loadgen/Dockerfile`, `python:3.11-slim-bookworm`)
pins its Python deps in `docker/loadgen/requirements.txt`:
`psycopg2-binary` 2.9.9, `boto3` 1.35.0, `Faker` 30.0.0.

Container images used by the manifests:

| Manifest | Image | Tag |
|---|---|---|
| `10-seaweedfs.yaml` | `chrislusf/seaweedfs` | `4.37` |
| `20-bucket-init.yaml` | `minio/mc` | `latest` |
| `50-postgres.yaml` | `postgres` | `16` |
| `30-iceberg-rest.yaml` | `iceberg-rest` (built locally) | `local` |
| `40-spark-iceberg.yaml` | `spark-iceberg` (built locally) | `local` |
| `60-loadgen.yaml` | `loadgen` (built locally) | `local` |
| `70-trino.yaml` | `trinodb/trino` | `482` |
| `80-metabase.yaml` | `metabase` (built locally) | `local` |

The `iceberg-rest` image is built from `docker/iceberg-rest/Dockerfile`, which
extends `apache/iceberg-rest:1.10.1` and adds the Postgres JDBC driver for
persistent table metadata via JdbcCatalog. The base image tag tracks the Iceberg
jar version in the Spark image (both `1.10.1`) so the catalog and Spark agree on
the table spec.

Init containers use `busybox:1.38` (in `30-iceberg-rest.yaml`,
`40-spark-iceberg.yaml`, `70-trino.yaml`, and `80-metabase.yaml`).

> `minio/mc` still uses the floating `latest` tag (it publishes date-stamped
> `RELEASE.*` tags rather than semver). Pin it to a specific `RELEASE.*` tag for
> fully reproducible runs.

## Spark catalog — `docker/spark/spark-defaults.conf`

`pyspark` launches the JVM via `spark-submit`, which reads this file, so any
`SparkSession.builder.getOrCreate()` in a notebook picks up the `demo` catalog
automatically. Key settings:

| Setting | Value | Notes |
|---|---|---|
| catalog name | `demo` | also `spark.sql.defaultCatalog` |
| `spark.sql.catalog.demo.type` | `rest` | Iceberg REST catalog |
| `…demo.uri` | `http://iceberg-rest:8181` | in-cluster DNS |
| `…demo.warehouse` | `s3://warehouse/` | matches the bootstrapped bucket |
| `…demo.io-impl` | `org.apache.iceberg.aws.s3.S3FileIO` | S3 data path |
| `…demo.s3.endpoint` | `http://seaweedfs:8333` | SeaweedFS S3 gateway |
| `…demo.s3.path-style-access` | `true` | **required** for SeaweedFS |
| `spark.sql.extensions` | `IcebergSparkSessionExtensions` | enables Iceberg SQL |
| `spark.driver.memory` | `1g` | kept small to fit a kind worker |
| `spark.sql.shuffle.partitions` | `4` | small-data default |
| `spark.eventLog.*` | `/home/iceberg/spark-events` | for the Spark UI |
| `spark.hadoop.fs.s3a.endpoint` | `http://seaweedfs:8333` | `s3a://` reads (raw pageview JSON) |
| `spark.hadoop.fs.s3a.path.style.access` | `true` | **required** for SeaweedFS |
| `spark.hadoop.fs.s3a.connection.ssl.enabled` | `false` | plain-HTTP in-cluster |

The `fs.s3a.*` block is only used by the pipeline's raw-JSON read
(`20_s3_to_bronze.py` / notebook `02`); Iceberg table I/O uses the `demo.s3.*`
settings above instead.

## PyIceberg — `docker/spark/pyiceberg.yaml`

Copied to `/root/.pyiceberg.yaml`. Defines a catalog named `default` pointing at
the same REST endpoint and SeaweedFS credentials, plus `s3.region: us-east-1`, so
you can `load_catalog("default")` without Spark.

## Credentials

A single static S3 identity is used everywhere:

- **Access key / secret:** `admin` / `password`
- Defined in `k8s/10-seaweedfs.yaml` (the `seaweedfs-s3-config` ConfigMap,
  `s3.json`) with full access.
- Consumed by the REST catalog and Spark as `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` env vars, and baked into `spark-defaults.conf` and
  `pyiceberg.yaml`.
- Region is fixed at `us-east-1`; SeaweedFS ignores it but the AWS SDK requires
  one.

**Jupyter runs with no token and no password** (`entrypoint.sh`:
`--ServerApp.token='' --ServerApp.password=''`) and `allow_origin='*'`. This is
deliberate for a throwaway local lab. Do not expose any of this to a network,
and do not reuse the credentials elsewhere.

## Storage sizes

| PVC | Size | Manifest |
|---|---|---|
| `seaweedfs-data` | 2 Gi | `10-seaweedfs.yaml` |
| `notebooks` | 1 Gi | `40-spark-iceberg.yaml` |
| `postgres-data` | 1 Gi | `50-postgres.yaml` |
| `metabase-data` | 1 Gi | `80-metabase.yaml` |

Both use kind's default `standard` StorageClass (the local-path provisioner).
SeaweedFS is also capped by `-master.volumeSizeLimitMB=1024`. Bump these if you
plan to load larger datasets.

## Resource requests / limits

| Workload | CPU request | Memory request | Memory limit |
|---|---|---|---|
| `seaweedfs` | 100m | 256Mi | 1Gi |
| `iceberg-rest` | 100m | 384Mi | 1Gi |
| `spark-iceberg` | 250m | 1Gi | 3Gi |
| `postgres` | 100m | 256Mi | 1Gi |
| `loadgen` (Job) | 100m | 128Mi | 512Mi |
| `trino` (opt-in) | 250m | 2Gi | 3Gi |
| `metabase` (opt-in) | 100m | 1Gi | 2Gi |

## Pinning the Kubernetes version

`cluster/kind-config.yaml` uses whatever node image ships with your kind
release. To pin it, add an `image:` to each node, e.g.:

```yaml
nodes:
  - role: control-plane
    image: kindest/node:v1.36.1
    extraPortMappings: [ ... ]
  - role: worker
    image: kindest/node:v1.36.1
  - role: worker
    image: kindest/node:v1.36.1
```
