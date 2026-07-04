# Metabase — BI dashboards on the Iceberg lakehouse.
#
# Extends the official Metabase image with the Starburst Trino driver so
# it can query Iceberg tables through Trino. The Trino driver JAR is not
# bundled in the official Metabase distribution — it must be added manually.
#
# This image is built and loaded into kind by `make serving`.
#
# Sticks with embedded H2 metadata storage (on a small PVC) to avoid the
# complexity of a dedicated Metabase metadata database. Single-pod only.
ARG METABASE_VERSION=v0.53.9

FROM metabase/metabase:${METABASE_VERSION}

# The base image runs as root by default. The Starburst Trino driver JAR goes
# into /plugins/ (rw for all by default).
ARG TRINO_DRIVER_VERSION=6.1.0
ADD https://github.com/starburstdata/metabase-driver/releases/download/${TRINO_DRIVER_VERSION}/starburst-${TRINO_DRIVER_VERSION}.metabase-driver.jar \
    /plugins/starburst-${TRINO_DRIVER_VERSION}.metabase-driver.jar
