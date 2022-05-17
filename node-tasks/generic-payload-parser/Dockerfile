ARG WORKER_BASE_IMAGE
FROM ${WORKER_BASE_IMAGE}

RUN apt-get update && apt-get install -y proj-bin

RUN mkdir /command
WORKDIR /command

COPY package.json .
COPY package-lock.json .
RUN npm install --production

COPY index.js .
COPY lib lib

CMD node /service