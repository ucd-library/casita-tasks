ARG NODEJS_BASE
FROM ${NODEJS_BASE}

RUN apt-get update && apt-get install -y ssh

RUN mkdir /root/.ssh/
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan grb-box.cstars.ucdavis.edu >> /root/.ssh/known_hosts

RUN mkdir /service
WORKDIR /service

COPY package.json .
COPY package-lock.json .
RUN npm install

COPY index.js .
COPY default-kafka-setup.js .
COPY test.js .
COPY run.sh .

ENV GRB_FILE ${GRB_FILE}
ENV SSH_KEY_USERNAME ${SSH_KEY_USERNAME}

CMD ./run.sh