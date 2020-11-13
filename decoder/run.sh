#! /bin/bash

ssh-keyscan grb-box.cstars.ucdavis.edu >> /root/.ssh/known_hosts
ssh ${SSH_KEY_USERNAME}@grb-box.cstars.ucdavis.edu "tail -f /grb/${GRB_FILE}/grbpackets.dat" | node test.js