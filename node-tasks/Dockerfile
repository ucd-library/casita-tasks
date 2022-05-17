ARG AIRFLOW_WORKER_BASE=apache/airflow:2.2.4
ARG NODE_VERSION=16

### MAIN IMAGE ###
FROM ${AIRFLOW_WORKER_BASE}
ARG NODE_VERSION

USER root

# Install node & other apt dependencies
RUN curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash -

RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        postgis nodejs imagemagick proj-bin build-essential

RUN mkdir /casita
WORKDIR /casita

COPY package.json .
COPY package-lock.json .
RUN npm install --production

COPY ./block-ring-buffer /casita/block-ring-buffer
COPY ./external-topics /casita/external-topics
COPY ./generic-payload-parser /casita/generic-payload-parser
COPY ./node-image-utils /casita/node-image-utils

USER airflow