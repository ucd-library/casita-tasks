FROM postgres:13

ENV POSTGRES_HOST_AUTH_METHOD=trust
ENV POSTGRES_DB=casita
ENV POSTGIS_ENABLE_OUTDB_RASTERS=1
ENV POSTGIS_GDAL_ENABLED_DRIVERS=ENABLE_ALL

RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        postgis \
        postgresql-13-postgis-3 \
        postgresql-13-postgis-3-scripts \
      && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./initdb-postgis.sql /docker-entrypoint-initdb.d/postgis.sql