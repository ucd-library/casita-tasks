FROM node:14

RUN apt-get update && apt-get install -y ssh

RUN mkdir /root/.ssh/
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan grb-box.cstars.ucdavis.edu >> /root/.ssh/known_hosts

RUN mkdir /service
WORKDIR /service

RUN npm install --save @ucd-lib/goes-r-packet-decoder@0.1.1
COPY index.js .
COPY test.js .

ENV GRB_FILE ${GRB_FILE}
ENV SSH_KEY_USERNAME ${SSH_KEY_USERNAME}

CMD bash -c "ssh ${SSH_KEY_USERNAME}@grb-box.cstars.ucdavis.edu 'tail -f /grb/${GRB_FILE}/grbpackets.dat' | node index.js"