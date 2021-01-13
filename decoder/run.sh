#! /bin/bash

if [[ -f "/root/.ssh/id_rsa" ]]; then
  echo "Updating /root/.ssh/id_rsa permissions"
  chmod 600 /root/.ssh/id_rsa 
fi
ls -al /root/.ssh
ssh-keyscan grb-box.cstars.ucdavis.edu >> /root/.ssh/known_hosts
ssh ${SSH_KEY_USERNAME}@grb-box.cstars.ucdavis.edu "tail -f /grb/${GRB_FILE}/grbpackets.dat" | node index.js