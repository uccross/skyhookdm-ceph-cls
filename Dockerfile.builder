ARG CEPH_RELEASE="v14.2.9"

FROM ceph/ceph:${CEPH_RELEASE}

ARG CEPH_RELEASE

# gcc, cmake3, arrow, parquet
RUN yum -y install https://packages.endpoint.com/rhel/7/os/x86_64/endpoint-repo-1.7-1.x86_64.rpm && \
    yum install -y \
      bc \
      git \
      centos-release-scl \
      cmake3 && \
    yum install -y \
      devtoolset-8 && \
    # ceph build deps \
    git clone --depth=1 --branch=${CEPH_RELEASE} https://github.com/ceph/ceph && \
    cd ceph && \
    bash -c '. /opt/rh/devtoolset-8/enable && ./install-deps.sh' && \
    # arrow, parquet \
    yum install -y https://apache.bintray.com/arrow/centos/7/apache-arrow-release-latest.rpm && \
    yum install -y --enablerepo=epel \
      arrow-devel \
      parquet-devel && \
    yum clean all

COPY builder_entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
