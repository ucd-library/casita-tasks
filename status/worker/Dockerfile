ARG WORKER_BASE_IMAGE
FROM ${WORKER_BASE_IMAGE}

RUN mkdir /command
WORKDIR /command

COPY package.json .
COPY package-lock.json .
RUN npm install

COPY bin bin
COPY lib lib
COPY index.js .
RUN npm install -g

CMD node /command