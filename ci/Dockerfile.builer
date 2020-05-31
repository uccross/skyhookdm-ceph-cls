FROM centos:7.8.2003

ARG CEPH_RELEASE="v14.2.9"

# gcc, cmake3, arrow, parquet
RUN yum -y install https://packages.endpoint.com/rhel/7/os/x86_64/endpoint-repo-1.7-1.x86_64.rpm && \
    yum install -y \
      bc \
      git \
      centos-release-scl \
      python-virtualenv && \
    yum install -y \
      devtoolset-8 && \
    # ceph build deps \
    git clone --depth=1 --branch=${CEPH_RELEASE} https://github.com/ceph/ceph && \
    cd ceph && \
    bash -c '. /opt/rh/devtoolset-8/enable && ./install-deps.sh' && \
    # re2, arrow, parquet \
    yum install -y https://apache.bintray.com/arrow/centos/7/apache-arrow-release-latest.rpm && \
    yum install -y \
      re2-devel \
      arrow-devel \
      parquet-devel && \
    yum clean all

COPY builder_entrypoint.sh /

ENTRYPOINT ["/builder_entrypoint.sh"]
