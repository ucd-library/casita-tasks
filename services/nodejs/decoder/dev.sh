#! /bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $ROOT_DIR

# Dev files can be found at:
# Location: us-central1
# Project: casita-298223
# gs://casita-test-data/decoded.dat
# gs://casita-test-data/secdecoded.dat

echo "running dev stream from local file ${GRB_FILE}.dat"
more /storage/network/${GRB_FILE}.dat | node lib/dev.js