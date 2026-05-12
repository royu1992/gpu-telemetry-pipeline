# Thin wrapper around the official Postgres 16 image.
# The database schema is auto-migrated at startup by the Collector service
# (store.Migrate()), so no custom init scripts are required here.
FROM postgres:16-alpine

ENV LANG=en_US.UTF-8 \
    POSTGRES_DB=telemetry \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres

EXPOSE 5432
