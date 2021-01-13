#! /bin/bash

if [[ -f "/root/.ssh/id_rsa" ]]; then
  chmod 600 /root/.ssh/id_rsa 
fi
ssh-keyscan grb-box.cstars.ucdavis.edu >> /root/.ssh/known_hosts
ssh ${SSH_KEY_USERNAME}@grb-box.cstars.ucdavis.edu "tail -f /grb/${GRB_FILE}/grbpackets.dat" | node index.js