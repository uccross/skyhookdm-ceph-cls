#!/bin/bash

TEST_DIR="test"
CONF_DIR="/etc/ceph"

# boot a single-osd instance
scripts/micro-osd.sh $TEST_DIR $CONF_DIR

# copy conf to where test expects it (PWD)
cp $CONF_DIR/ceph.conf ./

# run test
cls_sdk_test
