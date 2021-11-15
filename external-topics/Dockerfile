ARG WORKER_BASE_IMAGE
FROM ${WORKER_BASE_IMAGE}

RUN mkdir /command
WORKDIR /command

COPY package.json .
# COPY package-lock.json .
RUN npm install

COPY index.js .
RUN npm link

CMD node /command