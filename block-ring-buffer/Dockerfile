ARG WORKER_BASE_IMAGE
FROM ${WORKER_BASE_IMAGE}

RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        postgis \
      && rm -rf /var/lib/apt/lists/*

RUN mkdir /command
WORKDIR /command

COPY package.json .
COPY package-lock.json .
RUN npm install

COPY lib lib
COPY index.js .
RUN npm link

CMD node /command