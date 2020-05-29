# SkyhookDM Ceph RADOS Class

This repository is NOT intended to be cloned directly. Instead, it is configured 
as a submodule of <https://github.com/uccross/skyhookdm-ceph>. To clone the parent repo: 

```bash
git clone --recursive --depth 1 --branch skyhookdm-nautilus https://github.com/uccross/skyhookdm-ceph
```

and this repo will be available at `src/cls/tabular` within the parent 
`skyhookdm-ceph` repository.

We go into the parent repo first, before proceeding with the following steps:

```bash
cd skyhookdm-ceph
```

## build the builder

```bash
docker build -t builder -f src/cls/tabular/Dockerfile.builder src/cls/tabular/
```

## build the library

```bash
cd skyhookdm-ceph
docker run --rm -ti -v $PWD:/ws -w /ws builder
```

## package the library

```bash
docker build -t myorg/skyhook:nautilus-centos7 .
docker push myorg/skyhook:nautilus-centos7
```

## try it locally

**TODO**

## on kubernetes (via rook)

**TODO**
