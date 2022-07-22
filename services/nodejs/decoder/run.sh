#! /bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $ROOT_DIR

if [[ -e "/root/ssh-key/ssh.key" ]]; then
  echo "Copying /root/ssh-key/ssh.key to /root/.ssh/id_rsa"
  cp /root/ssh-key/ssh.key /root/.ssh/id_rsa 
fi

if [[ -e "/root/.ssh/id_rsa" ]]; then
  echo "Updating /root/.ssh/id_rsa permissions"
  chmod 600 /root/.ssh/id_rsa 
fi

ssh-keyscan grb-box.cstars.ucdavis.edu >> /root/.ssh/known_hosts
ssh ${SSH_KEY_USERNAME}@grb-box.cstars.ucdavis.edu "tail -F /grb/${GRB_FILE}/grbpackets.dat" | node index.js