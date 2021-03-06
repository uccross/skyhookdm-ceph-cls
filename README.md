# SkyhookDM Ceph RADOS Class

[![Build Status](https://travis-ci.com/uccross/skyhookdm-ceph-cls.svg?branch=master)](https://travis-ci.com/uccross/skyhookdm-ceph-cls)
[![CROSS](https://img.shields.io/badge/supported%20by-CROSS-green)](https://cross.ucsc.edu)

## What is SkyhookDM?

SkyhookDM extends Ceph distributed object storage with data management functionality for tabular data.  Data is partitioned, and partitions are stored in Ceph objects.  Data management methods are applied directly within the storage system, via the object-level, i.e., 'cls' interface.  Methods include both typical push-downs (offloaded to storage) for query processing such as SELECT, PROJECT, AGGREGATE, and also more general data management techniques such as indexing and physical design, including data layouts and formats.  Data partitions are currently stored as Apache Arrow or Google Flatbuffers format within objects.

## Try it locally

**TODO**

## On Kubernetes (via [Rook](https://rook.io))

**TODO**

## Questions

For questions, please ask about SkyhookDM on [StackOverflow](https://stackoverflow.com/tags/skyhook-ceph) with the tag `[skyhook-ceph]`

## Dev setup

These instructions explain the container-native development setup for 
SkyhookDM. With this approach, there's no need to install build 
dependencies, and instead we download a Docker image containing the 
toolchain that builds, tests and packages the library. For information 
on how to install Docker, take a look at [the official 
documentation][docker-install].

In addition, instead of making use of `docker` commands directly, we 
automate and document these tasks using Popper. You can think of 
Popper as `make` for containers. The Popper tasks defined for this 
project (`dev-init`, `build`, `test`, and `build-rook-img`) are 
defined in the [`.popper.yml`](.popper.yml) file. For information on 
how to install Popper, take a look at [the official 
documentation][popper-install].

Any of the tasks defined in the `.popper.yml` file can be executed in 
interactive mode. For example, to open a shell on the `build` step:

```bash
popper sh build
```

The above opens an interactive shell inside an instance of the 
[builder image](./ci/Dockerfile), which is a pre-built image with all 
the dependencies needed to build the CLS.

Note that each task depends on having executed the previous one at 
least once. In the example above, we must have executed the `dev-init` 
task.

[docker-install]: https://docs.docker.com/get-docker/
[popper-install]: https://github.com/getpopper/popper/blob/master/README.md#installation

### `dev-init`

```bash
popper run dev-init
```

The above clones ceph and creates symlinks to this project within the 
ceph tree so that the cls folder is referenced to the right place 
(`ceph/src/cls/tabular` within the cloned `ceph/` tree is a symlink to 
the root of this project).

### `build`

Build the skyhookdm library:

```bash
popper run build
```

The above builds the code inside the `build/` directory generated by 
CMake inside the `ceph/` folder, as one would normally expect.

### interactive `build`

To interactively build the RADOS class, or run tests within this dev 
environment, we can open a shell in this build container for this 
step:

```bash
popper sh build
```

Then, inside the container, we can run tests by doing:

```bash
cd ceph/build

# build
make -j4

vstart.sh

# TODO: complete it
# - create pools
# - run bin/ceph_test_skyhook_query
# - etc
```

### `build-rook-img`

Once the `build` step has been executed, whether in interactive 
(`popper sh`) or non-interactive (`popper run`) way, this next step 
generates a rook-compatible docker image:

```bash
popper run build-rook-img
```

The image contains, in addition to upstream ceph packages, the 
`libcls_tabular.so`, and auxiliary binaries so that container 
instances of this image can be deployed as OSDs and be able to run the 
tabular class methods.

### `test`

The tests execute on the image built previously (the rook image). This 
is done so that we can test that the SkyhookDM RADOS class can be 
loaded in an upstream installation. In addition, this step also 
produces an image that can be uploaded to a container image registry 
that Rook can pull from.

To run tests on the `ceph/` folder, you can run the `build` step in 
interactive mode as described previously.

```bash
popper run test
```
