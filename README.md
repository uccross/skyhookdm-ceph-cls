# SkyhookDM Ceph RADOS Class

[![Build Status](https://travis-ci.com/uccross/skyhookdm-ceph-cls.svg?branch=master)](https://travis-ci.com/uccross/skyhookdm-ceph-cls)

## What is SkyhookDM?

**TODO**

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
automate and self-document these tasks using Popper. You can think of 
Popper as `make` for containers. The Popper tasks defined for this 
project (`dev-init`, `build`, `test`, and `build-rook-img`) are 
defined in the [`.popper.yml`](.popper.yml) file. For information on 
how to install Popper, take a look at [the official 
documentation][popper-install].

Any of the tasks defined in `.popper.yml` can be executed in 
interactive mode. For example, to open a shell on the `build` step:

```bash
popper sh build
```

The above opens an interactive shell inside an instance of the 
[builder image](./ci/Dockerfile), which is a pre-built image with all 
the dependencies needed to build the CLS.

[docker-install]: https://docs.docker.com/get-docker/
[popper-install]: https://github.com/getpopper/popper/blob/master/README.md#installation

### build

```bash
popper run dev-init
```

The above clones ceph and creates symlinks to this project within the 
ceph tree so that the cls folder is copied to the right place.

### build the library

```bash
popper run build
```

### run tests

```bash
popper run test
```

### generate a rook-compatible docker image

```bash
popper run build-rook-img
```

