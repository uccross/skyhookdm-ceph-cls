#!/bin/bash
set -ex

TEST_DATA_LINK="https://users.soe.ucsc.edu/~jlefevre/skyhookdb/testdata/"
TEST_OBJS_BIN="obj000000"
TEST_OBJS_ARROW="skyhook.SFT_ARROW.lineitem"
TEST_OBJS_FLATBUF="skyhook.SFT_FLATBUF_FLEX_ROW.lineitem"
TEST_OBJS_JSON="skyhook.SFT_JSON.lineitem"

# download test data if it doesn't exist
if [[ ! -d ./data ]]; then
  mkdir -p data
  pushd data/
  for i in {0..9}; do
    TEST_DATA_FILE=${TEST_DATA_LINK}${TEST_OBJS_BIN}$i.bin
    curl -LO ${TEST_DATA_FILE}
  done
  for i in {0..1}; do
    curl -LO ${TEST_DATA_LINK}${TEST_OBJS_ARROW}.$i
    curl -LO ${TEST_DATA_LINK}${TEST_OBJS_FLATBUF}.$i
    curl -LO ${TEST_DATA_LINK}${TEST_OBJS_JSON}.$i
  done
  popd
fi

TEST_DIR="test"
CONF_DIR="/etc/ceph"

# boot a single-osd instance
scripts/micro-osd.sh $TEST_DIR $CONF_DIR

# copy conf to where test expects it (PWD)
cp $CONF_DIR/ceph.conf ./

# run test
ceph_test_skyhook_query
