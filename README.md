# SkyhookDM Ceph RADOS Class

[![Build Status](https://travis-ci.com/uccross/skyhookdm-ceph-cls.svg?branch=master)](https://travis-ci.com/uccross/skyhookdm-ceph-cls)

## dev setup

Install [Docker][docker-install] and [Popper][popper-install]. Then:

```bash
popper run dev-init
```

The above clones ceph and creates symlinks to this project. For more 
see the [`.popper.yml`](.popper.yml) file.

[docker-install]: https://docs.docker.com/get-docker/
[popper-install]: https://github.com/getpopper/popper/blob/master/docs/sections/getting_started.md#installation

## build the library

```bash
popper run build
```

## generate a rook-compatible docker image

```bash
popper run build-rook-img
```

## interactive shell

Any of the commands used above can be executed in interactive mode. 
For example, to open a shell on the `build` step:

```bash
popper sh build
```

The above opens an interactive shell inside an instance of the 
[builder image](./ci/Dockerfile), which is a pre-built image with all 
the dependencies needed to build the CLS.

## try it locally

**TODO**

## on kubernetes (via [rook](https://rook.io))

**TODO**

## Questions 

For questions, please ask about SkyhookDM on [StackOverflow](https://stackoverflow.com/tags/skyhook-ceph) with the tag `[skyhook-ceph]`
