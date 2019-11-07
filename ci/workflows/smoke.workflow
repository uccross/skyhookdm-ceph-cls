workflow "smoke tests" {
  resolves = "run tests"
}

action "build skyhook cls" {
  uses = "docker://popperized/ceph-builder:skyhook-bionic"
  args = "cls_tabular run-query ceph_test_skyhook_query"
  env = {
    CMAKE_FLAGS = "-DCMAKE_BUILD_TYPE=MinSizeRel -DWITH_RBD=OFF -DWITH_CEPHFS=OFF -DWITH_RADOSGW=OFF -DWITH_LEVELDB=OFF -DWITH_MANPAGE=OFF -DWITH_RDMA=OFF -DWITH_OPENLDAP=OFF -DWITH_FUSE=OFF -DWITH_LIBCEPHFS=OFF -DWITH_KRBD=OFF -DWITH_LTTNG=OFF -DWITH_BABELTRACE=OFF -DWITH_SYSTEMD=OFF -DWITH_SPDK=OFF -DWITH_CCACHE=ON -DBOOST_J=4"
    BUILD_THREADS = "4"
  }
}

action "download test data" {
  needs = "build skyhook cls"
  uses = "popperized/bin/curl@master"
  runs = ["sh", "-c", "ci/scripts/download-test-data.sh"]
}

<<<<<<< HEAD
<<<<<<< HEAD
# NOTE: done offline to speedup travis build time

action "build ceph image" {
  needs = "download test data"
  uses = "actions/docker/cli@master"
  args = "build -t popperized/ceph:luminous ci/docker"
}

action "registry login" {
  needs = "build ceph image"
  uses = "actions/docker/login@master"
  secrets = ["DOCKER_USERNAME", "DOCKER_PASSWORD"]
}

action "push image" {
  needs = "registry login"
  uses = "actions/docker/cli@master"
  args = "push popperized/ceph:luminous"
}

action "run tests" {
  needs = "push image"
#  needs = "download test data"
  uses = "docker://popperized/ceph:luminous"
  runs = [
    "sh", "-c", "ci/scripts/run-skyhook-test.sh"
=======
=======
# build an image with upstream ceph-mon/ceph-osd packages and add skyhook
<<<<<<< HEAD
# runtime dependencies such as libarrow and libhdf5
>>>>>>> Adds comments on contents of Ceph image
=======
# runtime dependencies such as libarrow, libhdf5, etc
>>>>>>> Use ceph builder image
action "build ceph image" {
  needs = "download test data"
  uses = "popperized/docker/cli@master"
  args = "build -t popperized/ceph:luminous ci/docker"
}

action "run tests" {
  needs = "build ceph image"
  uses = "popperized/docker/cli@master"
  runs = [
    "sh", "-c",
<<<<<<< HEAD
    "docker", "run", "--rm",
    "--volume", "$GITHUB_WORKSPACE:/ws",
    "--workdir=/ws",
    "--entrypoint=/ws/ci/scripts/run-skyhook-test.sh",
    "popperized/ceph:luminous",
>>>>>>> Builds image for ceph as part of workflow
=======
    "docker run --rm --volume $GITHUB_WORKSPACE:/ws --workdir=/ws --entrypoint=/ws/ci/scripts/run-skyhook-test.sh popperized/ceph:luminous"
>>>>>>> Fixes invocation of docker run
  ]
}
