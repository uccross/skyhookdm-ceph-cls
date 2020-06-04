#!/bin/bash
set -ex

# download test data if it doesn't exist
if [[ ! -d ./data ]]; then
  mkdir -p data
  pushd data/
  for i in $(seq 0 9); do
    curl -LO https://users.soe.ucsc.edu/~jlefevre/skyhookdb/testdata/obj000000$i.bin
  done
  popd
fi

# boot a single-osd instance
scripts/micro-osd.sh test

# copy conf to where test expects it (PWD)
cp test/ceph.conf ./

# run test
ceph_test_skyhook_query
