ARG WORKER_BASE_IMAGE
FROM ${WORKER_BASE_IMAGE}

RUN mkdir /command
WORKDIR /command

COPY package.json .
COPY package-lock.json .
RUN npm install --production

COPY index.js .

CMD node /service