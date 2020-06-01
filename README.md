# SkyhookDM Ceph RADOS Class

This repository is **NOT** intended to be cloned directly. Instead, it 
is configured as a submodule of 
<https://github.com/uccross/skyhookdm-ceph>. To clone the parent repo:

```bash
git clone \
  --recursive \
  --shallow-submodules \
  --depth 1 \
  --branch skyhookdm-luminous \
  https://github.com/uccross/skyhookdm-ceph
```

and this repo will be available at `src/cls/tabular` within the parent 
`skyhookdm-ceph` repository. All the following steps assume we're 
inside the repository:

```bash
cd skyhookdm-ceph
```

## build the builder

> **NOTE**: we keep images in the [uccross/skyhookdm-builder][skydh] 
> dockerhub repository that can be used to build the library (see next section).
> We include this step for completeness but you can skip it and use 
> images from this dockerhub repo instead.

[skydh]: https://hub.docker.com/r/uccross/skyhookdm-builder

```bash
docker build \
  --tag=uccross/skyhookdm-builder:luminous \
  --build-arg=CEPH_VERSION=luminous \
  --file=ci/Dockerfile.builder \
  ci/
```

## build the library

```bash
docker run --rm -ti \
  -e CMAKE_FLAGS='-DBOOST_J=16' \
  -e BUILD_THREADS=16 \
  -v $PWD:/ws \
  -w /ws \
  uccross/skyhookdm-builder:luminous \
    cls_tabular
```

In the above, the `-DBOOST_J=16` setting is controls the number of build
jobs used to build boost. The `BUILD_THREADS=16` controls the number of
jobs for building SkyhookDM library.

## package the library

**TODO**

## try it locally

**TODO**

## on kubernetes (via [rook](https://rook.io))

**TODO**
