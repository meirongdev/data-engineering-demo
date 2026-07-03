# syntax=docker/dockerfile:1
#
# Iceberg REST catalog server with Postgres JDBC driver for persistent
# table metadata via JdbcCatalog.
#
# The base apache/iceberg-rest image ships the REST server fat jar but does
# NOT include the Postgres JDBC driver (it's a runtime-scope dependency).
# We download it here and drop it into the classpath.
FROM alpine:3.20 AS downloader

ARG POSTGRES_JDBC_VERSION=42.7.6

RUN apk add --no-cache curl && \
    curl -fSL \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o /jdbc/postgresql-${POSTGRES_JDBC_VERSION}.jar

FROM apache/iceberg-rest:1.10.1

ARG POSTGRES_JDBC_VERSION=42.7.6

# The base image's entrypoint uses "libs/*" in the classpath, so any jar
# placed in /opt/iceberg-rest/libs/ is picked up automatically.
COPY --from=downloader /jdbc/postgresql-${POSTGRES_JDBC_VERSION}.jar \
     /opt/iceberg-rest/libs/postgresql-${POSTGRES_JDBC_VERSION}.jar
